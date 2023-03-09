package ru.qu8.rocketmq.comnmsg;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.IOException;
import java.util.UUID;

public class AsyncProducer {
    public static void main(String[] args) throws MQClientException{
        DefaultMQProducer mqProducer = new DefaultMQProducer("Common_Message");
        mqProducer.setNamesrvAddr("150.230.38.161:9876");
        mqProducer.start();

        for (int i = 0;i< 10;i++){
            Message message = new Message();
            message.setBody("It's a common async producer msg.....".getBytes());
            message.setTopic("CommonMessage");
            message.setKeys("OrderID-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
            message.setTags("AsyncMsg");

            try {
                mqProducer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("Async message send ok");
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("Async message send failed");
                    }
                });
            } catch (RemotingException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        mqProducer.shutdown();
    }
}
