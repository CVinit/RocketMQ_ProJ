package ru.qu8.rocketmq.comnmsg;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.UUID;

public class SyncMsg {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer mqProducer = new DefaultMQProducer("Common_Message");
        mqProducer.setNamesrvAddr("150.230.38.161:9876");
        mqProducer.setRetryTimesWhenSendFailed(3);
        mqProducer.setSendMsgTimeout(3000);
        mqProducer.start();

        for (int i = 0;i< 50;i++){
            Message message = new Message();
            message.setBody("It's a common sync producer msg.....".getBytes());
            message.setTopic("CommonMessage");
            message.setKeys("OrderID-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
            message.setTags("SyncMsg");

            SendResult sendResult = mqProducer.send(message);

            System.out.println(sendResult);
        }

        mqProducer.shutdown();
    }
}
