package ru.qu8.rocketmq.txmsg;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TXListener implements TransactionListener {
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("预提交消息成功："+msg);

        if (StringUtils.equals("TAGA",msg.getTags())){
            return LocalTransactionState.COMMIT_MESSAGE;
        }else if (StringUtils.equals("TAGB",msg.getTags())){
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else if (StringUtils.equals("TAGC",msg.getTags())) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("执行消息回查" + msg.getTags());
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
