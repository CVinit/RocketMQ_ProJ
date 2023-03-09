package ru.qu8.rocketmq.comnmsg;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.List;

public class ComnConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("Common_Consumer");
        consumer.setNamesrvAddr("150.230.35.86:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("CommonMessage", "AsyncMsg");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.start();

        try {
            while (true) {
                List<MessageExt> messageExts = consumer.poll();
                for (MessageExt messageExt : messageExts){
                    System.out.println(messageExt);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            consumer.shutdown();
        }
        /*consumer.setMessageQueueListener(new MessageQueueListener() {
            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                for (MessageQueue mq : mqAll){
                    System.out.println("Topic:"+topic+", msg:"+mq.toString());
                }
            }
        });*/
    }
}
