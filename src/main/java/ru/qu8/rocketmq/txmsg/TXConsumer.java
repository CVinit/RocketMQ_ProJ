package ru.qu8.rocketmq.txmsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class TXConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer txConsumer = new DefaultMQPushConsumer("TX_Consumer");
        txConsumer.setNamesrvAddr("150.230.35.86:9876");
        txConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        txConsumer.subscribe("TXMessage", MessageSelector.byTag("*"));

        txConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msgExt:msgs) {
                    System.out.println(msgExt);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        txConsumer.start();
        System.out.println("Consumer Started");
    }
}
