package com.kano.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


public class ProducerDelay {


    @Test
    public void tesDelay() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("delay-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(1);

        producer.start();
        String body = "delay-kano-%s";
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 1; i++) {
            Message message = new Message("delay-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 30s后投递给消费者
            message.setDelayTimeLevel(4);
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
