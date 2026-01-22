package com.example.billingbatch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisProducerConfig {

  @Bean
  public StreamOperations<String, String, String> streamOps(
      StringRedisTemplate redisTemplate) {
    return redisTemplate.opsForStream();
  }
}
