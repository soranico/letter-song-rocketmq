package com.kano.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.List;


public class ProducerOffset {


    @Test
    public void testLastOffset() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("last-offset-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);

        producer.start();
        String body = "last-offset-%s";
        for (int i = 0; i < 5; i++) {
            Message message = new Message("last-offset-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            if (i == 0) {
                producer.sendOneway(message);
            } else if (i != 2) {
                SendResult sendResult = producer.send(message);
                System.out.printf("message id = %s, queue = %s, offset = %s%n",
                        sendResult.getMsgId(),
                        sendResult.getMessageQueue(), sendResult.getQueueOffset());
            } else {
                producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("message id = %s, queue = %s, offset = %s%n",
                                sendResult.getMsgId(),
                                sendResult.getMessageQueue(), sendResult.getQueueOffset());
                    }

                    @Override
                    public void onException(Throwable e) {

                    }
                });
            }
        }
        producer.shutdown();
    }


    @Test
    public void testQueueOrder() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("queue-order-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");

        producer.start();
        String body = "queue-order-kano-%s";
        for (int i = 0; i < 8; i++) {
            Message message = new Message("queue-order-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    int cur = Integer.parseInt(arg.toString());
                    return mqs.get(cur % mqs.size());
                }
            }, i);
            System.out.printf("message id = %s, queue = %s, offset = %s%n",
                    sendResult.getMsgId(),
                    sendResult.getMessageQueue(), sendResult.getQueueOffset());
        }


        producer.shutdown();
    }

}
