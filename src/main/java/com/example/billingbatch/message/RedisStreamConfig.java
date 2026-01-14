package com.example.billingbatch.message;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;

@Configuration
public class RedisStreamConfig {

  @Bean
  public StreamMessageListenerContainer<String, MapRecord<String, String, String>>
  streamMessageListenerContainer(
      RedisConnectionFactory connectionFactory,
      MockMessageConsumer consumer
  ) {

    StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
        StreamMessageListenerContainerOptions
            .builder()
            .pollTimeout(Duration.ofSeconds(5))
            .build();

    StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
        StreamMessageListenerContainer.create(connectionFactory, options);

    container.receive(
        Consumer.from("message-group", "consumer-1"),
        StreamOffset.create("message-stream", ReadOffset.lastConsumed()),
        consumer
    );

    container.start();
    return container;
  }
}
