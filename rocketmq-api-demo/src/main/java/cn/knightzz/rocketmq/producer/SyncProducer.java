package cn.knightzz.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author 王天赐
 * @title: SyncProducer
 * @projectName message-queue
 * @description:
 * @website <a href="http://knightzz.cn/">http://knightzz.cn/</a>
 * @github <a href="https://github.com/knightzz1998">https://github.com/knightzz1998</a>
 * @create: 2023-02-06 15:08
 */
public class SyncProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("pg");

        // 指定nameserver地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 设置发送失败时重新发送的次数, 默认是2次
        producer.setRetryTimesWhenSendFailed(3);
        // 设置发送超时时间, 默认3s
        producer.setSendMsgTimeout(5000);

        // 开启生产者
        producer.start();

        // 同步消息发送
        // 同步发送消息是指，Producer发出⼀条消息后，会在收到MQ返回的ACK之后才发下⼀条消息。该方式
        // 的消息可靠性最高，但消息发送效率太低

        for (int i = 1; i < 100; i++) {
            String msg = "我是" + i + "学生";
            byte[] body = msg.getBytes();
            Message message = new Message("SchoolTopic", "StudentTag", body);
            // 为当前的消息指定 key
            message.setKeys("key-" + i);

            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
