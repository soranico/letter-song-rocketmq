package com.kano.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ProducerBatch {


    /**
     * TODO 一批都会在同个队列？
     */
    @Test
    public void tesBatch() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("batch-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);

        producer.start();
        String body = "batch-kano-%s";
        CountDownLatch latch = new CountDownLatch(1);
        List<Message> batchMessage = new ArrayList<>(5);
        for (int i = 0; i < 2; i++) {
            /**
             * 一批消息不能超过4M
             * 不能有延时消息
             */
            for (int j = 0; j < 5; j++) {
                Message message = new Message("batch-topic",
                        String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                batchMessage.add(message);
            }

            producer.send(batchMessage, new SendCallback() {
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
