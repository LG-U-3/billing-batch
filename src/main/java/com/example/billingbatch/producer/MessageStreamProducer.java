package com.example.billingbatch.producer;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Component;

@Component
public class MessageStreamProducer {

  @Value("${redis.stream.message.key}")
  private String streamKey;

  public String streamKey() {
    return streamKey;
  }

  private final StreamOperations<String, String, String> streamOps;

  public MessageStreamProducer(StreamOperations<String, String, String> streamOps) {
    this.streamOps = streamOps;
  }

  public void publish(Long messageSendResultId, String channel, String purpose) {
    streamOps.add(
        streamKey,
        Map.of(
            "messageSendResultId", messageSendResultId.toString(),
            "channel", channel,
            "purpose", purpose
        )
    );
  }
}