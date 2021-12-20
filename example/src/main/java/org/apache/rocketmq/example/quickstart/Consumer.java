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
package org.apache.rocketmq.example.quickstart;

import java.util.List;
import java.util.Set;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quick-consumer-group");

        consumer.setNamesrvAddr("127.0.0.1:9876");



        consumer.setMessageModel(MessageModel.CLUSTERING);
//        consumer.setMessageModel(MessageModel.BROADCASTING);

        /**
         * 从最新的消息开始消费
         * 这个参数对于老消费者是不会生效的,因为老消费者的消费进度
         * 会存放到
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeTimestamp("20211215000000");

        consumer.subscribe("quickTopic", "*");
        consumer.setConsumeMessageBatchMaxSize(2);

        consumer.setPollNameServerInterval(30000);

        consumer.setHeartbeatBrokerInterval(30000);

        consumer.setPersistConsumerOffsetInterval(50000);

        // 取模平均A
        consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragely());

        consumer.setConsumeThreadMin(20);

        consumer.setConsumeThreadMax(20);

        /**
         * 并发消费情况下,单个queue的消费最大的跨度
         * 超过这个值会引发限流策略
         */
        consumer.setConsumeConcurrentlyMaxSpan(2000);

        /**
         * 单个queue缓存到本地最大的数据
         */
        consumer.setPullThresholdForQueue(1000);

        /**
         * 拉取消息的间隔
         * 这个间隔是这次请求响应后下次请求的间隔
         * 也就是必须的等上次请求结束
         */
        consumer.setPullInterval(0);

        consumer.setPullBatchSize(32);
        /**
         * 批量消费的最大条数 也就是一次消费多少条
         * 这个和一次最大拉取多少条不同
         * 这个是 register的回调里面的list的数据量
         * 实际值<=
         */
        consumer.setConsumeMessageBatchMaxSize(1);


        /**
         * 默认里面只有一条消息
         * 返回 success 表明这条消息消费成功
         * 对于并发而言如果异常或者 RECONSUME_LATER
         * 都会重新把消息发给broker 会发送到 RETRY 队列
         *
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });



        consumer.start();

        /**
         * 主题下的队列的数量
         */
        Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues("quickTopic");

        System.out.printf("Consumer Started.%n");
    }
}
