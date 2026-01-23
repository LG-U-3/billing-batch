package com.example.billingbatch.recovery;

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.example.billingbatch.common.code.CodeCache;
import com.example.billingbatch.common.code.enums.CodeGroups;
import com.example.billingbatch.common.code.enums.MessageSendStatus;
import com.example.billingbatch.recovery.dao.MessageSendResultJdbcDao;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class ProcessingTimeoutScheduler {

  private static final Logger log = LoggerFactory.getLogger(ProcessingTimeoutScheduler.class);

  private final MessageSendResultJdbcDao dao;
  private final CodeCache codeCache;

  @Value("${message.recovery.timeout-minutes:60}")
  private long timeoutMinutes;

  @Value("${message.recovery.db-batch-size:500}")
  private int dbBatchSize;

  private Long STATUS_PROCESSING;
  private Long STATUS_FAILED;

  private volatile boolean initialized = false;

  private void initIfNeeded() {
    if (initialized) return;

    this.STATUS_PROCESSING = codeCache.getId(
        CodeGroups.MESSAGE_SEND_STATUS.name(),
        MessageSendStatus.PROCESSING.name()
    );

    this.STATUS_FAILED = codeCache.getId(
        CodeGroups.MESSAGE_SEND_STATUS.name(),
        MessageSendStatus.FAILED.name()
    );

    initialized = true;
    log.info("[INIT] ProcessingTimeoutScheduler status ids initialized");
  }

  @Scheduled(fixedDelayString = "${message.recovery.fixed-delay-ms:300000}")
  @Transactional
  public void recoverProcessingTimeout() {
    log.info("[SCHEDULER] db-timeout tick");

    initIfNeeded();

    LocalDateTime threshold = LocalDateTime.now().minusMinutes(timeoutMinutes);

    List<Long> ids = dao.findProcessingTimeoutIds(STATUS_PROCESSING, threshold, dbBatchSize);

    if (ids == null || ids.isEmpty()) {
      return;
    }

    int updated = dao.markFailedByIds(ids, STATUS_PROCESSING, STATUS_FAILED);

    log.warn("[TIMEOUT] PROCESSING -> FAILED recovered. threshold={}, idsCount={}, updated={}",
        threshold, ids.size(), updated);
  }
}
