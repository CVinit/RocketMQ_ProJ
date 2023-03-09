package ru.qu8.rocketmq.sequencemsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class PushConsumer {
    public static void main(String[] args) {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("Push_Consumer");
        try {
            pushConsumer.setNamesrvAddr("150.230.35.86:9876");
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            pushConsumer.subscribe("SequenceMessage","SyncMsg");

            pushConsumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    for (MessageExt msgExt : msgs){
                        System.out.println(msgExt.getQueueId()+","+msgExt.getMsgId()+","+msgExt.getBodyCRC());
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            pushConsumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } finally {
        }


    }
}
