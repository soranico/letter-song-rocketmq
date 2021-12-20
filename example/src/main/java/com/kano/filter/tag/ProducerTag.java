package com.kano.filter.tag;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


public class ProducerTag {


    @Test
    public void tesTag() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tag-filter-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);
        String [] tags = new String[]{"TAG-A","TAG-B","TAG-C"};
        producer.start();
        String body = "tag-filter-kano-%s";
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 5; i++) {
            Message message = new Message("tag-filter-topic",tags[i% tags.length],
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
