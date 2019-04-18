/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";

    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<>();
    private AtomicBoolean updateLock = new AtomicBoolean();

    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     *
     * @param invokers 此处我自己理解为provider的接口
     * @param invocation 本次调用的一些参数
     * @return Collection<String>
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    /**
     *
     * @param invokers 接口
     * @param url 在此轮询方法中 url没有用到
     * @param invocation 方法 传参
     * @param <T>
     */
    @Override
    @SuppressWarnings("Duplicates")
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //每个invoker都是一样的 我们关注的是要用哪一个实例
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map == null) {
            methodWeightMap.putIfAbsent(key, new ConcurrentHashMap<>());
            map = methodWeightMap.get(key);
        }
        int totalWeight = 0;
        long maxCurrent = Long.MIN_VALUE;
        long now = System.currentTimeMillis();
        Invoker<T> selectedInvoker = null;
        WeightedRoundRobin selectedWRR = null;
        for (Invoker<T> invoker : invokers) {
            //identifyString-------------
            // dubbo://10.29.196.121:20883/com.netfinworks.pfs.service.payment.BalancePaymentFacade?application=pfs-payment&default.export=true&dubbo=scpay-2.6.9&export=true&generic=false&interface=com.netfinworks.pfs.service.payment.BalancePaymentFacade&pid=22190&revision=2.2.0&side=provider×tamp=1554992340991
            String identifyString = invoker.getUrl().toIdentityString(); //这个URL不是net包里的url
            WeightedRoundRobin weightedRoundRobin = map.get(identifyString);
            int weight = getWeight(invoker, invocation);
            if (weightedRoundRobin == null) {
                weightedRoundRobin = new WeightedRoundRobin();
                weightedRoundRobin.setWeight(weight);
                map.putIfAbsent(identifyString, weightedRoundRobin);
            }
            if (weight != weightedRoundRobin.getWeight()) {
                //weight changed
                weightedRoundRobin.setWeight(weight);
            }
            //这个cur是选择权重 就是是不是选过的一个记录
            long cur = weightedRoundRobin.increaseCurrent();
            weightedRoundRobin.setLastUpdate(now);
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }
            totalWeight += weight;
        }
        //此处为什么会不相等 invokers.size() != map.size()
        //文档的解释
        //           对 <identifyString, WeightedRoundRobin> 进行检查，过滤掉长时间未被更新的节点。--map被干掉了一个
        //        // 该节点可能挂了，invokers 中不包含该节点，所以该节点的 lastUpdate 长时间无法被更新。 -- invoker少了
        //        // 若未更新时长超过阈值后，就会被移除掉，默认阈值为60秒。 -- map被干掉一个
        if (!updateLock.get() && invokers.size() != map.size()) {

            if (updateLock.compareAndSet(false, true)) {
                try {
                    // copy -> modify -> update reference
                    //copyOnWrite的思想
                    ConcurrentMap<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<>(map);
                    Iterator<Entry<String, WeightedRoundRobin>> it = newMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, WeightedRoundRobin> item = it.next();
                        int RECYCLE_PERIOD = 60000;
                        //长时间没有更新 则移除了
                        if (now - item.getValue().getLastUpdate() > RECYCLE_PERIOD) {
                            it.remove();
                        }
                    }
                    methodWeightMap.put(key, newMap);
                } finally {
                    updateLock.set(false);
                }
            }
        }

        //算法核心很简单 就是已经被选中的 会被猛减去一次权重 那么下次可以轮给别人
        if (selectedInvoker != null) {
            selectedWRR.sel(totalWeight);  //减去的是总权重
            return selectedInvoker;
        }
        // should not happen here
        return invokers.get(0);
    }


    /**
     * 这个是文档2.6.4版本中的IntegerWrapper
     */
    protected static class WeightedRoundRobin {
        private int weight;
        // TODO: 2019/4/18 current什么用de
        private AtomicLong current = new AtomicLong(0);
        private long lastUpdate;

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0); //表明这是一个新的current
        }

        private long increaseCurrent() {
            return current.addAndGet(weight);
        }

        private void sel(int total) {
            current.addAndGet(-1 * total);
        }

        private long getLastUpdate() {
            return lastUpdate;
        }

        private void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

}
