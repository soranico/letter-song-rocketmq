package com.kano.concurrent;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ProducerConcurrent {



    @Test
    public void tesConcurrent() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("concurrent-producer-group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);

        producer.setCompressMsgBodyOverHowmuch(10);


        producer.start();
        String body = "concurrent-kano-%s";
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 1; i++) {
            Message message = new Message("concurrent-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("message id = %s, queue = %s, offset = %s%n",
                            sendResult.getMsgId(),
                            sendResult.getMessageQueue(), sendResult.getQueueOffset());
                    latch.countDown();
                }

                @Override
                public void onException(Throwable e) {

                }
            });

        }
        latch.await();
        producer.shutdown();
    }


    @Test
    public void tesFailed() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("concurrent-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);

        producer.start();
        String body = "concurrent-kano-%s";
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 1; i++) {
            Message message = new Message("concurrent-failed-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("message id = %s, queue = %s, offset = %s%n",
                            sendResult.getMsgId(),
                            sendResult.getMessageQueue(), sendResult.getQueueOffset());
                    latch.countDown();
                }

                @Override
                public void onException(Throwable e) {

                }
            });

        }
        latch.await();
        producer.shutdown();
    }




}
