package com.example.billingbatch.service;

import com.example.billingbatch.common.code.CodeCache;
import com.example.billingbatch.common.code.enums.MessageSendStatus;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.example.billingbatch.producer.MessageStreamProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageRetryService {

  private final JdbcTemplate jdbcTemplate;
  private final MessageStreamProducer messageStreamProducer;
  private final CodeCache codeCache;

  private Long STATUS_PROCESSING;
  private Long STATUS_FAILED;
  private Long STATUS_EXCEEDED;

  private static final int MAX_EMAIL_RETRY_COUNT = 2;
  private static final String PURPOSE_BILLING = "BILLING";

  private static final String CHANNEL_EMAIL = "EMAIL";
  private static final String CHANNEL_SMS = "SMS";

  @EventListener(ApplicationReadyEvent.class)
  void initCodes() {
    this.STATUS_PROCESSING =
        codeCache.getId("MESSAGE_SEND_STATUS", MessageSendStatus.PROCESSING.name());
    this.STATUS_FAILED =
        codeCache.getId("MESSAGE_SEND_STATUS", MessageSendStatus.FAILED.name());
    this.STATUS_EXCEEDED =
        codeCache.getId("MESSAGE_SEND_STATUS", MessageSendStatus.EXCEEDED.name());
  }

  public int retryFailedMessages(int limit) {
    log.info("[retryFailedMessages] started");
    int emailRepublished = republishEmailRetries(limit);
    int smsRepublished = republishSmsFallback(limit);

    return emailRepublished + smsRepublished;
  }

  // FAILED(BILLING) → PROCESSING (bulk) → EMAIL 재실행
  private int republishEmailRetries(int limit) {

    LocalDateTime batchStart = LocalDateTime.now();
    int updated = jdbcTemplate.update("""
            UPDATE message_send_results msr
            SET
              msr.status_id    = ?,
              msr.requested_at = CURRENT_TIMESTAMP,
              msr.processed_at = NULL
            WHERE msr.id IN (
              SELECT id FROM (
                SELECT msr2.id
                FROM message_send_results msr2
                JOIN message_templates mt ON mt.id = msr2.template_id
                JOIN codes pu ON pu.id = mt.purpose_type_id
                WHERE msr2.status_id = ?
                  AND pu.code = ?
                  AND msr2.retry_count < ?
                  AND msr2.processed_at IS NOT NULL
                ORDER BY msr2.processed_at
                LIMIT ?
              ) t
            )
            """,
        STATUS_PROCESSING,
        STATUS_FAILED,
        PURPOSE_BILLING,
        MAX_EMAIL_RETRY_COUNT,
        limit
    );

    log.info("[republishEmailRetries] bulk update count={}", updated);
    if (updated == 0) {
      return 0;
    }

    List<Long> ids = jdbcTemplate.queryForList("""
            SELECT msr.id
              FROM message_send_results msr
             WHERE msr.status_id = ?
               AND msr.processed_at IS NULL
            """,
        Long.class,
        STATUS_PROCESSING
    );

    for (Long id : ids) {
      messageStreamProducer.publish(id, CHANNEL_EMAIL, PURPOSE_BILLING);
    }

    return ids.size();
  }

  // EXCEEDED(BILLING) → PROCESSING (bulk) → SMS fallback
  private int republishSmsFallback(int limit) {

    LocalDateTime batchStart = LocalDateTime.now();
    int updated = jdbcTemplate.update("""
            UPDATE message_send_results msr
            SET
              msr.status_id    = ?,
              msr.requested_at = CURRENT_TIMESTAMP,
              msr.processed_at = NULL
            WHERE msr.id IN (
              SELECT id FROM (
                SELECT msr2.id
                FROM message_send_results msr2
                JOIN message_templates mt ON mt.id = msr2.template_id
                JOIN codes pu ON pu.id = mt.purpose_type_id
                WHERE msr2.status_id = ?
                  AND pu.code = ?
                  AND msr2.processed_at IS NOT NULL
                ORDER BY msr2.processed_at
                LIMIT ?
              ) t
            )
            """,
        STATUS_PROCESSING,
        STATUS_EXCEEDED,
        PURPOSE_BILLING,
        limit
    );

    log.info("[republishSmsFallback] bulk update count={}", updated);
    if (updated == 0) {
      return 0;
    }

    List<Long> ids = jdbcTemplate.queryForList("""
            SELECT msr.id
              FROM message_send_results msr
             WHERE msr.status_id = ?
               AND msr.processed_at IS NULL
            """,
        Long.class,
        STATUS_PROCESSING
    );

    for (Long id : ids) {
      messageStreamProducer.publish(id, CHANNEL_SMS, PURPOSE_BILLING);
    }

    return ids.size();
  }
}
