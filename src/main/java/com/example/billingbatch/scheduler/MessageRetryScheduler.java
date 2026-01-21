package com.example.billingbatch.scheduler;

import com.example.billingbatch.service.MessageRetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageRetryScheduler {

  private final MessageRetryService messageRetryService;

  @Scheduled(fixedDelay = 60_000, initialDelay = 5_000)
  public void retryFailed() {

    log.info("[MessageRetryScheduler] tick"); // 항상 찍기

    int count = messageRetryService.retryFailedMessages(1000);

    // 항상 찍기 (count=0이라도)
    log.info("[MessageRetryScheduler] republished count={}", count);
  }
}
