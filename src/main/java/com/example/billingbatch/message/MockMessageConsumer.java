package com.example.billingbatch.message;

import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class MockMessageConsumer
    implements StreamListener<String, MapRecord<String, String, String>> {

  @Override
  public void onMessage(MapRecord<String, String, String> message) {
    System.out.println("=== MOCK MESSAGE CONSUMED ===");
    System.out.println(message.getValue());
  }
}
