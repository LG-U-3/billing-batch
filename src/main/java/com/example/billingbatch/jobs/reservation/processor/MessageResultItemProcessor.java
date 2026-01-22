package com.example.billingbatch.jobs.reservation.processor;

import com.example.billingbatch.common.code.CodeCache;
import com.example.billingbatch.domain.MessageSendResult;
import com.example.billingbatch.jobs.reservation.processor.dto.ReservationInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MessageResultItemProcessor
    implements ItemProcessor<Long, MessageSendResult>, StepExecutionListener {

  private final JdbcTemplate jdbcTemplate;
  private final CodeCache codeCache;

  private ReservationInfo reservationInfo;
  private Long waitingStatusId;

  @Override
  public void beforeStep(StepExecution stepExecution) {

    ExecutionContext stepCtx = stepExecution.getExecutionContext();
    Long reservationId = stepCtx.getLong("reservationId");

    this.reservationInfo = jdbcTemplate.queryForObject(
        """
            SELECT
              r.id,
              r.template_id,
              r.channel_type_id,
              t.purpose_type_id
            FROM message_reservations r
            JOIN message_templates t ON r.template_id = t.id
            WHERE r.id = ?
            """,
        (rs, rowNum) -> new ReservationInfo(
            rs.getLong("id"),
            rs.getLong("template_id"),
            rs.getLong("channel_type_id"),
            rs.getLong("purpose_type_id")
        ),
        reservationId
    );

    this.waitingStatusId =
        codeCache.getId("MESSAGE_SEND_STATUS", "WAITING");
  }

  @Override
  public MessageSendResult process(Long userId) {
    return MessageSendResult.builder()
        .reservedSendId(reservationInfo.reservationId())
        .userId(userId)
        .templateId(reservationInfo.templateId())
        .channelId(reservationInfo.channelTypeId())
        .statusId(waitingStatusId)
        .build();
  }
}
