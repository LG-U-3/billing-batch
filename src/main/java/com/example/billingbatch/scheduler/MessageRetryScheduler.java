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

  @Scheduled(fixedDelay = 60_000, initialDelay = 10_000)
  public void retryFailed() {


    int count = messageRetryService.retryFailedMessages(10000);

  }
}
