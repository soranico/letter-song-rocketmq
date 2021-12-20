package com.kano.order;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.Test;

import java.util.List;
import java.util.Set;


public class ConsumerPull {


    /**
     * 并发无序
     * @throws Exception
     */
    @Test
    public void testGlobalOrderWithConcurrent() throws Exception{

        DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer("pull-quick-group");
        pullConsumer.subscribe("pull-quick-topic","*");
        pullConsumer.setMessageModel(MessageModel.CLUSTERING);
        pullConsumer.registerTopicMessageQueueChangeListener("pull-quick-topic", new TopicMessageQueueChangeListener() {
            @Override
            public void onChanged(String topic, Set<MessageQueue> messageQueues) {

            }
        });
        pullConsumer.poll(0);
        System.in.read();
    }





}
