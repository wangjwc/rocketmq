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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 是否开启故障规避处理策略
     *
     * 开启后，在发送消息时，会过滤掉not available的broker
     * （ps：not available的判断是根据前面发送失败的作故障规避处理。比如上次broker-a投递失败，则在30s内不在向该broker投递。具体规避时间按实际使用方式判断)
     */
    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 负载均衡：从所有队列中选择一个 TopicPublishInfo.messageQueueList
     * @param tpInfo
     * @param lastBrokerName 发送一条信息时，如果是重试则传值，否则是空, 用来避免重试时使用同一个broker
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            // 开启故障规避机制
            try {
                /*
                 * 递增后取模
                 */
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 满足条件则返回
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) { // 只使用可用的broker
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName)) // 防止和上次获取的broker一样
                            return mq;
                    }
                }

                /*
                 * 上面条件不满足（没有可用broker或者broker只有一个），则随机获取一个
                 */
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    // TODO 2021/6/8 下午2:19 这块是从指定broker中选择一个队列。。。不过先selectOneMessageQueue，在改队列id。。。这做法真令人迷惑
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 这可能是后来拉取broker信息的时候发现没有队列了（那就从避障策略里删掉）
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            // 前面一通操作都没取到，使用最基本的轮询
            return tpInfo.selectOneMessageQueue();
        }

        /**
         * 取模方式轮询选择队列
         */
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新broker发送延迟，和是否隔离(仅在sendLatencyFaultEnable==true时有效)
     * @param brokerName 本次投递的broker
     * @param currentLatency 本次投递broker耗时
     * @param isolation 是否隔离(隔离180秒，等待broker信息更新）（发送成功时false，失败时true）（ps：中断时也是false）
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            /*
             * 隔离：按30秒档次计算，即隔离180秒，以便等待broker信息更新
             * 非隔离：按实际延迟时间计算（即发送耗时越长，则越不优先使用）
             */
            // 30000 => 180000L
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 1、先使用latencyMax计算延迟属于哪个档次
     * 2、在返回notAvailableDuration中设置的该档次的避障时间
     * latencyMax =           {50L, 100L, 550L,   1000L,  2000L,   3000L,   15000L};
     * notAvailableDuration = {0L,  0L,   30000L, 60000L, 120000L, 180000L, 600000L};
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) // 判断延迟属于哪个档次
                return this.notAvailableDuration[i]; // 返回该档次的避障时间
        }

        return 0;
    }
}
