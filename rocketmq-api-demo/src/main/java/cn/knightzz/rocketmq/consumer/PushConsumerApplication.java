package cn.knightzz.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author 王天赐
 * @title: PushConsumerApplication
 * @projectName message-queue
 * @description:
 * @website <a href="http://knightzz.cn/">http://knightzz.cn/</a>
 * @github <a href="https://github.com/knightzz1998">https://github.com/knightzz1998</a>
 * @create: 2023-02-06 14:57
 */
public class PushConsumerApplication {

    public static void main(String[] args) throws MQClientException {

        // new DefaultMQPullConsumer();

        // 定义一个 Push 消息消费者 , PUll
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");

        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 指定消费的topic与tag
        consumer.subscribe("SchoolTopic", "*");

        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * 一旦 broker中有了其订阅的消息, 就立马触发该方法的执行
             * @param list 消息
             * @param consumeConcurrentlyContext
             * @return 其返回值为当前consumer消费的状态
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                            ConsumeConcurrentlyContext consumeConcurrentlyContext) {

                list.forEach((message) -> {
                    byte[] body = message.getBody();
                    System.out.println(new String(body));
                });

                // 返回消费状态 => 消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 开启消费者消费
        consumer.start();
        System.out.println("Consumer Started");

    }
}
