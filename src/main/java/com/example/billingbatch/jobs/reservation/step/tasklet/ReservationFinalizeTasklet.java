package com.example.billingbatch.jobs.reservation.step.tasklet;

import com.example.billingbatch.common.code.CodeCache;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@StepScope
@RequiredArgsConstructor
public class ReservationFinalizeTasklet
    implements Tasklet, StepExecutionListener {

  private final JdbcTemplate jdbcTemplate;
  private final CodeCache codeCache;

  private Long reservationId;

  @Override
  public void beforeStep(StepExecution stepExecution) {
    ExecutionContext context =
        stepExecution.getExecutionContext();

    this.reservationId = context.getLong("reservationId");
  }

  @Override
  @Transactional
  public RepeatStatus execute(
      StepContribution contribution,
      org.springframework.batch.core.scope.context.ChunkContext chunkContext
  ) {
    Long sentStatusId =
        codeCache.getId("RESERVATION_STATUS", "SENT");

    jdbcTemplate.update(
        "UPDATE message_reservations SET status_id = ? WHERE id = ?",
        sentStatusId,
        reservationId
    );

    return RepeatStatus.FINISHED;
  }
}
