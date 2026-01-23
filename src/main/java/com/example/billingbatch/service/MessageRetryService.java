package com.example.billingbatch.service;

import com.example.billingbatch.producer.MessageStreamProducer;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MessageRetryService {

  private final JdbcTemplate jdbcTemplate;
  private final MessageStreamProducer messageStreamProducer;

  // status_id
  private static final int FAILED = 9;
  private static final int EXCEEDED = 11;

  // 재시도는 무조건 EMAIL
  private static final String CHANNEL_EMAIL = "EMAIL";
  // fallback은 SMS
  private static final String CHANNEL_SMS = "SMS";

  public int retryFailedMessages(int limit) {

    int emailRepublished = republishEmailRetries(limit);
    int smsRepublished = republishSmsFallback(limit);

    return emailRepublished + smsRepublished;
  }

  // FAILED(9) + retry_count 0/1/2 는 "무조건 EMAIL"로 재발행
  private int republishEmailRetries(int limit) {
    String sql = """
        select
            msr.id as message_send_result_id,
            pu.code as purpose_code
        from message_send_results msr
        join message_templates mt on mt.id = msr.template_id
        join codes pu on pu.id = mt.purpose_type_id
        where msr.status_id = ?
          and msr.processed_at is not null
          and msr.retry_count in (0, 1, 2)
        order by msr.processed_at asc
        limit ?
        """;

    List<RetryTarget> targets = jdbcTemplate.query(sql,
        (rs, rowNum) -> new RetryTarget(rs.getLong("message_send_result_id"),
            rs.getString("purpose_code")),
        FAILED, limit);

    for (RetryTarget t : targets) {
      // 채널은 항상 EMAIL 강제
      messageStreamProducer.publish(t.messageSendResultId(), CHANNEL_EMAIL, t.purposeCode());
    }
    return targets.size();
  }

  // EXCEEDED(11)는 SMS fallback으로 발행
  private int republishSmsFallback(int limit) {
    String sql = """
        select
            msr.id as message_send_result_id,
            pu.code as purpose_code
        from message_send_results msr
        join message_templates mt on mt.id = msr.template_id
        join codes pu on pu.id = mt.purpose_type_id
        where msr.status_id = ?
          and msr.processed_at is not null
        order by msr.processed_at asc
        limit ?
        """;

    List<RetryTarget> targets = jdbcTemplate.query(sql,
        (rs, rowNum) -> new RetryTarget(rs.getLong("message_send_result_id"),
            rs.getString("purpose_code")),
        EXCEEDED, limit);

    for (RetryTarget t : targets) {
      messageStreamProducer.publish(t.messageSendResultId(), CHANNEL_SMS, t.purposeCode());
    }
    return targets.size();
  }

  private record RetryTarget(Long messageSendResultId, String purposeCode) {
  }
}
