package com.kano.delay;

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


public class ConsumerDelay {


    /**
     *
     * last_offset默认会从队列最后开始消费
     * 但对于老消费者是不生效,因为对于已经订阅的
     * 消费者组而言,这个组的消费进度被存放到 broker的
     * /store/config/consumerOffset.json 文件
     * 只要集群中的队列(可能是新增broker)没有发生改变
     * 那么这个记录就会存在,因此会从记录的位置开始消费
     *
     */
    @Test
    public void testDelay() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delay-consumer");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("delay-topic","*");
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
                System.err.printf("loop = %s",loop);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }





}
