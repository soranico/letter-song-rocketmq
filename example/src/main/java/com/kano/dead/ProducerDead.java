package com.kano.dead;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


public class ProducerDead {



    @Test
    public void testDeadMessage() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("dead-producer-group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);



        producer.start();
        String body = "dead-kano-%s";
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 1; i++) {
            Message message = new Message("dead-topic",
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
