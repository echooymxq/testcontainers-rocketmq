package com.echooymxq.testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.ClassRule;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class RocketMQTests {

    @ClassRule
    public static RocketMQContainer rocketMQContainer = new RocketMQContainer(DockerImageName.parse("apache/rocketmq:4.9.4"));

    private static final String TOPIC = "TopicTest";

    @Test
    public void testRocketMQ() throws Exception {
        final String namesrvAddr = rocketMQContainer.getNamesrvAddr();
        DefaultMQProducer producer = new DefaultMQProducer("testcontainers");
        producer.setNamesrvAddr(namesrvAddr);
        producer.start();
        Message msg = new Message(TOPIC, "Hello world".getBytes(StandardCharsets.UTF_8));
        SendResult sendResult = producer.send(msg);
        System.out.printf("%s%n", sendResult);

        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("testcontainers");
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(TOPIC, "*");
        consumer.start();

        Unreliables.retryUntilTrue(
            10,
            TimeUnit.SECONDS,
            () -> {
                List<MessageExt> messages = consumer.poll();
                assertThat(messages)
                    .hasSize(1)
                    .extracting(MessageExt::getTopic, MessageExt::getBody)
                    .containsExactly(tuple(TOPIC, "Hello world".getBytes(StandardCharsets.UTF_8)));

                return true;
            }
        );
        consumer.shutdown();
        producer.shutdown();
    }

}
