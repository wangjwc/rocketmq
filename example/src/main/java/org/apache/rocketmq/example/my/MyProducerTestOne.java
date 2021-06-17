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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

import static org.apache.rocketmq.example.my.MyProducerTest.start;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class MyProducerTestOne {
    public static final String NAME_SERVER_ADDR = "127.0.0.1:9876";
    public static final String TEST_PRODUCER_GROUP = "wjw_test_producer_group";
    public static final String TEST_TOPIC = "wjw_test_topic";


    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException, MQBrokerException {
        MQProducer producer = start();

        Message msg = new Message(TEST_TOPIC /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ " + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8) /* Message body */
        );
        msg.setWaitStoreMsgOK(true);

        try {
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
            producer.shutdown();
        }
    }
}
