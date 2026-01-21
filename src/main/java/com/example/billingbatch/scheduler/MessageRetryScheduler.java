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

  // FAILED 재시도 시간 조절
  @Scheduled(fixedDelay = 300_000, initialDelay = 5_000)
  public void retryFailed() {
    int count = messageRetryService.retryFailedMessages(1000);
    if (count > 0) {
      log.info("[MessageRetryScheduler] republished {} failed messages", count);
    }
  }
}
