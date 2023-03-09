package ru.qu8.rocketmq.sequencemsg;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

public class SequenceProducer {
    public static void main(String[] args) {
        DefaultMQProducer sequenceProducer = null;
        try {
            sequenceProducer = new DefaultMQProducer("Sequence_Producer");
            sequenceProducer.setNamesrvAddr("150.230.38.161:9876");
            sequenceProducer.setSendMsgTimeout(5000);
            sequenceProducer.start();

            for (int i =0 ;i < 10;i++){
                Integer orderID = i;

                Message msg = new Message();
                msg.setTopic("SequenceMessage");
                msg.setTags("SyncMsg");
                msg.setBody(("It's a sequence message..."+i).getBytes());

                SendResult sendResult = sequenceProducer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        if (mqs == null || mqs.isEmpty()){
                            return null;
                        }
                        return mqs.get(0);
                    }
                },orderID);

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
            sequenceProducer.shutdown();
        }


    }
}
