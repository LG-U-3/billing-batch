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

  // FAILED(status_id=9) 기준으로 재시도 대상 조회 후 publish
  // retry_count 0/1만 재발행 (2는 sender에서 "3번째 시도"로 SMS 강제전환 처리)
  public int retryFailedMessages(int limit) {
    String sql = """
        select
            msr.id as message_send_result_id,
            ch.code as channel_code,
            pu.code as purpose_code
        from message_send_results msr
        join message_templates mt on mt.id = msr.template_id
        join codes ch on ch.id = mt.channel_type_id
        join codes pu on pu.id = mt.purpose_type_id
        where msr.status_id = 9
          and msr.processed_at is not null
          and msr.retry_count in (0, 1)
        order by msr.processed_at asc
        limit ?
        """;

    List<RetryTarget> targets = jdbcTemplate.query(sql,
        (rs, rowNum) -> new RetryTarget(rs.getLong("message_send_result_id"),
            rs.getString("channel_code"), rs.getString("purpose_code")),
        limit);

    for (RetryTarget t : targets) {
      messageStreamProducer.publish(t.messageSendResultId(), t.channelCode(), t.purposeCode());
    }
    return targets.size();
  }

  private record RetryTarget(Long messageSendResultId, String channelCode, String purposeCode) {
  }
}
