package com.example.billingbatch.producer;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MessageStreamProducer {

  @Value("${redis.stream.message.key}")
  private String streamKey;

  private final StringRedisTemplate stringRedisTemplate;

  public void publish(Long messageSendResultId, String channel, String purpose) {
    StreamOperations<String, String, String> streamOps = stringRedisTemplate.opsForStream();

    streamOps.add(streamKey, Map.of("messageSendResultId", messageSendResultId.toString(),
        "channel", channel, "purpose", purpose));
  }
}
