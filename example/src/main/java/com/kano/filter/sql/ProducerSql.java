package com.kano.filter.sql;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


public class ProducerSql {


    @Test
    public void tesSql() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("sql-filter-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);
        String [] tags = new String[]{"TAG-A","TAG-B","TAG-C"};
        producer.start();
        String body = "sql-filter-kano-%s";
        int loop = 5;
        CountDownLatch latch = new CountDownLatch(loop);
        for (int i = 0; i < loop; i++) {
            Message message = new Message("sql-filter-topic",tags[i% tags.length],
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            message.putUserProperty("kano",String.valueOf(i));
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
