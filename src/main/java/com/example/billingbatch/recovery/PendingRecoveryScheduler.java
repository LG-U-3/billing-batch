package com.example.billingbatch.recovery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.example.billingbatch.common.code.CodeCache;
import com.example.billingbatch.common.code.enums.CodeGroups;
import com.example.billingbatch.common.code.enums.MessageSendStatus;
import com.example.billingbatch.producer.MessageStreamProducer;
import com.example.billingbatch.recovery.dao.MessageSendResultJdbcDao;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PendingRecoveryScheduler {

  private static final Logger log = LoggerFactory.getLogger(PendingRecoveryScheduler.class);

  private final StringRedisTemplate redisTemplate;
  private final MessageSendResultJdbcDao dao;
  private final MessageStreamProducer producer;
  private final CodeCache codeCache;

  @Value("${message.stream:message-stream}")
  private String streamKey;

  @Value("${message.group:message-group}")
  private String group;

  @Value("${message.recovery.timeout-minutes:60}")
  private long timeoutMinutes;

  @Value("${message.recovery.claim-count:100}")
  private int claimCount;

  // recovery용 consumer는 suffix로 구분
  @Value("${redis.stream.message.consumer:billing-batch}")
  private String baseConsumer;

  private Long STATUS_PROCESSING_ID;
  private Long STATUS_FAILED_ID;

  private volatile boolean initialized = false;

  private void initIfNeeded() {
    if (initialized) return;

    this.STATUS_PROCESSING_ID = codeCache.getId(
        CodeGroups.MESSAGE_SEND_STATUS.name(),
        MessageSendStatus.PROCESSING.name()
    );

    this.STATUS_FAILED_ID = codeCache.getId(
        CodeGroups.MESSAGE_SEND_STATUS.name(),
        MessageSendStatus.FAILED.name()
    );

    initialized = true;
    log.info("[INIT] PendingRecoveryScheduler status ids initialized");
  }

  private String recoveryConsumer() {
    return baseConsumer + "-recovery";
  }

  @Scheduled(fixedDelayString = "${message.recovery.fixed-delay-ms:300000}")
  @Transactional
  public void recoverPending() {
    log.info("[SCHEDULER] pending recovery tick");
    initIfNeeded();

    Duration minIdle = Duration.ofMinutes(timeoutMinutes);

    // 1) pending 요약(없으면 종료)
    PendingMessagesSummary summary = redisTemplate.opsForStream().pending(streamKey, group);
    if (summary == null || summary.getTotalPendingMessages() == 0) {
      return;
    }

    // 2) pending 상세 일부 조회
    PendingMessages candidates =
        redisTemplate.opsForStream().pending(streamKey, group, Range.unbounded(), claimCount);

    if (candidates == null || candidates.size() == 0) {
      return;
    }

    // 3) idle이 timeout 이상인 것만 claim
    List<RecordId> toClaim = new ArrayList<>();
    for (PendingMessage pm : candidates) {
      try {
        if (pm.getElapsedTimeSinceLastDelivery().compareTo(minIdle) >= 0) {
          toClaim.add(pm.getId());
        }
      } catch (Exception ignore) {}
    }

    if (toClaim.isEmpty()) {
      return;
    }

    // 4) XCLAIM: recovery consumer로 회수
    List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream().claim(
        streamKey,
        group,
        recoveryConsumer(),
        minIdle,
        toClaim.toArray(new RecordId[0])
    );

    if (claimed == null || claimed.isEmpty()) {
      return;
    }

    log.warn("[RECOVERY] claimed pending. count={}, consumer={}", claimed.size(), recoveryConsumer());

    // 5) 처리/ACK
    for (MapRecord<String, Object, Object> record : claimed) {
      RecordId recordId = record.getId();
      Map<Object, Object> payload = record.getValue();

      try {
        Long msrId = parseLong(payload.get("messageSendResultId"));
        String channel = toStr(payload.get("channel"));
        String purpose = toStr(payload.get("purpose"));

        if (msrId == null) {
          ack(recordId);
          log.warn("[RECOVERY] invalid payload(no msrId) -> ACK. recordId={}", recordId.getValue());
          continue;
        }

        String statusCode = dao.findStatusCodeByMsrId(msrId);

        if (statusCode == null) {
          // DB에 없으면 유령 메시지 정리
          ack(recordId);
          log.warn("[RECOVERY] status not found -> ACK only. msrId={}, recordId={}",
              msrId, recordId.getValue());
          continue;
        }

        // (1) 이미 확정 상태면 ACK만
        if (isFinalStatus(statusCode)) {
          ack(recordId);
          log.info("[RECOVERY] already finalized({}) -> ACK. msrId={}, recordId={}",
              statusCode, msrId, recordId.getValue());
          continue;
        }

        // (2) WAITING이면 재큐잉 후 ACK
        if ("WAITING".equals(statusCode)) {
          producer.publish(msrId, channel, purpose);
          ack(recordId);
          log.info("[RECOVERY] WAITING -> requeued + ACK. msrId={}, recordId={}",
              msrId, recordId.getValue());
          continue;
        }

        // (3) PROCESSING이면 FAILED 확정(선택지 A) 후 ACK
        if ("PROCESSING".equals(statusCode)) {
          int updated = dao.markFailedOne(msrId, STATUS_PROCESSING_ID, STATUS_FAILED_ID);
          ack(recordId);
          log.warn("[RECOVERY] PROCESSING -> FAILED(updated={}) + ACK. msrId={}, recordId={}",
              updated, msrId, recordId.getValue());
          continue;
        }

        // (4) 그 외는 ACK로 정리
        ack(recordId);
        log.warn("[RECOVERY] unknown status({}) -> ACK only. msrId={}, recordId={}",
            statusCode, msrId, recordId.getValue());

      } catch (Exception e) {
        // 예외면 ACK 안 함 → 다음 주기 재시도
        log.error("[RECOVERY] failed to handle claimed record. recordId={}", recordId.getValue(), e);
      }
    }
  }

  private boolean isFinalStatus(String statusCode) {
    return "SUCCESS".equals(statusCode)
        || "FAILED".equals(statusCode)
        || "EXCEEDED".equals(statusCode);
  }

  private void ack(RecordId id) {
    redisTemplate.opsForStream().acknowledge(streamKey, group, id);
  }

  private static Long parseLong(Object v) {
    if (v == null) return null;
    try { return Long.valueOf(String.valueOf(v)); }
    catch (Exception e) { return null; }
  }

  private static String toStr(Object v) {
    return v == null ? null : String.valueOf(v);
  }
}
