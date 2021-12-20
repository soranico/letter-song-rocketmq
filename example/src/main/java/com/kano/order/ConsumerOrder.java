package com.kano.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.List;
import java.util.Set;


public class ConsumerOrder {


    /**
     * 并发无序
     * @throws Exception
     */
    @Test
    public void testGlobalOrderWithConcurrent() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("global-consumer-group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("global-order","*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("message id = %s, queue = %s, queue offset = %s, log offset = %s %n",
                            msg.getMsgId(),
                            msg.getQueueId(), msg.getQueueOffset(),msg.getCommitLogOffset());
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    /**
     * 有序监听才是真正的有序
     * @throws Exception
     */
    @Test
    public void testGlobalOrder() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("global-consumer-group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("global-order","*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("message id = %s, queue = %s, queue offset = %s, log offset = %s %n",
                            msg.getMsgId(),
                            msg.getQueueId(), msg.getQueueOffset(),msg.getCommitLogOffset());
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues("global-order");
        System.out.printf("consumer queue size = %s %n",messageQueueSet.size());
        System.in.read();
    }


    @Test
    public void testQueueOrder() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("global-consumer-group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("queue-order-topic","*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("message id = %s, queue = %s, queue offset = %s, log offset = %s %n",
                            msg.getMsgId(),
                            msg.getQueueId(), msg.getQueueOffset(),msg.getCommitLogOffset());
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();

        System.in.read();
    }


    /**
     * 顺序消息如果消费失败或者抛出异常
     * 那么这个消息不会重新投递而是会稍后继续消费
     *
     * 如果这个消息一直无法成功,那么也不会进入死信队列
     * 而是一直无限的重试,因为顺序消息每个消息都要消费
     * 完成,所以不能像并发消息一样跳过这个消息稍后投递
     * 或者投递16次也没有成功后进入死信队列
     */
    @Test
    public void testOrderWithFailed() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("global-consumer-group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("global-order","*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("message id = %s, queue = %s, queue offset = %s, log offset = %s %n",
                            msg.getMsgId(),
                            msg.getQueueId(), msg.getQueueOffset(),msg.getCommitLogOffset());
                }
                int i  = 1/0;
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });
        consumer.start();
        Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues("global-order");
        System.out.printf("consumer queue size = %s %n",messageQueueSet.size());
        System.in.read();
    }




}
