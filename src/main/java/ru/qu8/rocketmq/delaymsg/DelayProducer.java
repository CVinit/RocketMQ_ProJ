package ru.qu8.rocketmq.delaymsg;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class DelayProducer {
    public static void main(String[] args) {
        DefaultMQProducer delayProducer = new DefaultMQProducer("Delay_Producer");
        try {
            delayProducer.setNamesrvAddr("150.230.38.161:9876");
            delayProducer.start();

            for (int i = 0;i<10;i++){
                Message message = new Message("DelayMessage", "SyncMsg", "It's an delay msg...".getBytes());
                message.setDelayTimeLevel(3);

                SendResult sendResult = delayProducer.send(message);

                System.out.println(sendResult);
            }
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } catch (MQBrokerException e) {
            throw new RuntimeException(e);
        } catch (RemotingException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            delayProducer.shutdown();
        }
    }
}
