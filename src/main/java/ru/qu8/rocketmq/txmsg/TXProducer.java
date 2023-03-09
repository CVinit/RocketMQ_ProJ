package ru.qu8.rocketmq.txmsg;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.*;

public class TXProducer {
    public static void main(String[] args) {
        TransactionMQProducer txProducer = new TransactionMQProducer("TX_Producer");
        try {
            txProducer.setNamesrvAddr("150.230.38.161:9876");

            ExecutorService executorService = new ThreadPoolExecutor( 2 , 5 ,100 , TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>( 2000 ), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("client-transaction-msg-check-thread");
                    return thread;
                }
            });

            txProducer.setExecutorService(executorService);
            txProducer.setTransactionListener(new TXListener());
            txProducer.start();

            String[] tags = {"TAGA","TAGB","TAGC"};
            for (int i = 0; i < 3; i++) {
                Message message = new Message("TXMessage", tags[i], "It's a tx msg....".getBytes());
                TransactionSendResult transactionSendResult = txProducer.sendMessageInTransaction(message,null);
                System.out.println("发送结果为：" +transactionSendResult.getSendStatus());
            }
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } finally {
        }

    }
}
