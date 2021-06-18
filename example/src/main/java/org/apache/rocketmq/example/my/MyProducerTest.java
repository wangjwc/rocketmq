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
package org.apache.rocketmq.example.my;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class MyProducerTest {
    public static final String NAME_SERVER_ADDR = "127.0.0.1:9876";
    public static final String TEST_PRODUCER_GROUP = "wjw_test_producer_group";
    public static final String TEST_TOPIC = "wjw_test_topic";

    public static DefaultMQProducer start() throws MQClientException {
        System.setProperty("rocketmq.client.logUseSlf4j", "true");

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(TEST_PRODUCER_GROUP);
        //producer.createTopic("", "", 1);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);
        // 是否开启故障规避处理策略
        producer.setSendLatencyFaultEnable(true);
        producer.start();
        return producer;
    }

    public static void main(String[] args) throws MQClientException, InterruptedException {
        MQProducer producer = start();

        for (int i = 0; i < 10; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message(TEST_TOPIC /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(StandardCharsets.UTF_8) /* Message body */
                );
                msg.setWaitStoreMsgOK(true);

                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /**
         * 异步
         */
        try {
            Message msg = new Message(TEST_TOPIC /* Topic */,
                    "TagA" /* Tag */,
                    ("async message").getBytes(StandardCharsets.UTF_8) /* Message body */
            );

            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {

                }
                @Override
                public void onException(Throwable e) {

                }
            });
        } catch (RemotingException e) {
            e.printStackTrace();
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
