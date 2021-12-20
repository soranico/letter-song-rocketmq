package com.kano.transaction;

import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.transaction.TransactionProducer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class ProducerTransaction {


    /**
     *
     * 事务消息的生产者群组是一种保证高可用的机制
     * 当发送事务消息的 生产者宕机后,broker会回查群组中的
     * 其他生产者的事务状态来保证事务的完整性
     *
     */
    @Test
    public void tesTransaction01() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);

        ProducerTransactionListener transactionListener = new ProducerTransactionListener();
        producer.setTransactionListener(transactionListener);
        AtomicInteger ctl = new AtomicInteger(1);
        ThreadPoolExecutor transactionThreadPool = new ThreadPoolExecutor(2, 2,
                0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(32),
                run -> {
                    Thread thread = new Thread(run);
                    thread.setName("transaction-thread-pool-" + ctl.getAndIncrement());
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());

        producer.setExecutorService(transactionThreadPool);

        producer.start();
        String body = "transaction-kano-%s";
        int loop = 1;
        CountDownLatch latch = new CountDownLatch(loop);
        for (int i = 0; i < loop; i++) {
            Message message = new Message("transaction-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            /**
             * 发送消息后多少秒开始回查事务的状态
             */
            message.putUserProperty("CHECK_IMMUNITY_TIME_IN_SECONDS","1");
            producer.sendMessageInTransaction(message, null);
        }
        latch.await();
        producer.shutdown();
    }



    @Test
    public void tesTransaction02() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setDefaultTopicQueueNums(4);

        ProducerTransactionListener transactionListener = new ProducerTransactionListener();
        producer.setTransactionListener(transactionListener);
        AtomicInteger ctl = new AtomicInteger(1);
        ThreadPoolExecutor transactionThreadPool = new ThreadPoolExecutor(2, 2,
                0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(32),
                run -> {
                    Thread thread = new Thread(run);
                    thread.setName("transaction-thread-pool-" + ctl.getAndIncrement());
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());

        producer.setExecutorService(transactionThreadPool);

        producer.start();
        String body = "transaction-kano-%s";
        int loop = 0;
        CountDownLatch latch = new CountDownLatch(loop);
        for (int i = 0; i < loop; i++) {
            Message message = new Message("transaction-topic",
                    String.format(body, i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            /**
             * 发送消息后多少秒开始回查事务的状态
             */
            message.putUserProperty("CHECK_IMMUNITY_TIME_IN_SECONDS","1");
            producer.sendMessageInTransaction(message, null);
        }
        latch.await();
        System.in.read();
    }


    private static class ProducerTransactionListener implements TransactionListener {

        private AtomicInteger ctl = new AtomicInteger(0);
        private ConcurrentMap<String ,Integer> messageStatusMap = new ConcurrentHashMap<>();

        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            int status = ctl.getAndIncrement();
            messageStatusMap.put(msg.getTransactionId(),status%3);
            return LocalTransactionState.UNKNOW;
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            Integer status = messageStatusMap.get(msg.getTransactionId());
            if (status == null){
                System.out.printf("t = %s , msg = %s  not found %n",Thread.currentThread().getName(),msg.getTransactionId());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            switch (status){
                case 0:
                    System.out.printf("t = %s , msg = %s  unknown %n",Thread.currentThread().getName(),msg.getTransactionId());
                    return LocalTransactionState.UNKNOW;
                case 1:
                    System.out.printf("t = %s , msg = %s  commit %n",Thread.currentThread().getName(),msg.getTransactionId());
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    System.out.printf("t = %s , msg = %s rollback %n",Thread.currentThread().getName(),msg.getTransactionId());
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                default:
                    System.out.printf("t = %s , msg = %s not found %n",Thread.currentThread().getName(),msg.getTransactionId());
                    return LocalTransactionState.COMMIT_MESSAGE;
            }
        }
    }


}
