package ru.qu8.rocketmq.delaymsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class DelayConsumer {
    public static void main(String[] args) {
        DefaultMQPushConsumer delayConsumer = new DefaultMQPushConsumer("Delay_Consumer");
        try {
            delayConsumer.setNamesrvAddr("150.230.35.86:9876");
            delayConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            delayConsumer.subscribe("SequenceMessage","SyncMsg");

            delayConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msgExt:msgs) {
                        System.out.println(msgExt);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            delayConsumer.start();
//            Thread.sleep(20000L);
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } finally {
//            delayConsumer.shutdown();
        }
    }
}
