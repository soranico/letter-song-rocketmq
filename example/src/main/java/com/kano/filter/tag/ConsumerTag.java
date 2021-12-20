package com.kano.filter.tag;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class ConsumerTag {



    @Test
    public void testTag() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-filter-consumer");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("tag-filter-topic","TAG-A || TAG-B");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        AtomicInteger ctl = new AtomicInteger();
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                int loop = ctl.incrementAndGet();
                for (MessageExt msg : msgs) {
                    System.out.printf("message id = %s, queue = %s, queue offset = %s, log offset = %s %n",
                            msg.getMsgId(),
                            msg.getQueueId(), msg.getQueueOffset(),msg.getCommitLogOffset());
                }
                System.err.printf("loop = %s%n",loop);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }





}
