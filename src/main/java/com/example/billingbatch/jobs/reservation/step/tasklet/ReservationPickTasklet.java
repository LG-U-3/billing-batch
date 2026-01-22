package com.example.billingbatch.jobs.reservation.step.tasklet;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import com.example.billingbatch.common.code.CodeCache;

@Component
@RequiredArgsConstructor
public class ReservationPickTasklet implements Tasklet {

  private final JdbcTemplate jdbcTemplate;
  private final CodeCache codeCache;

  @Override
  @Transactional
  public RepeatStatus execute(
      StepContribution contribution,
      ChunkContext chunkContext
  ) {

    Long waitingStatusId =
        codeCache.getId("RESERVATION_STATUS", "WAITING");
    Long processingStatusId =
        codeCache.getId("RESERVATION_STATUS", "PROCESSING");

    // 처리 대상 예약 조회
    List<Long> candidateIds = jdbcTemplate.queryForList(
        """
            SELECT id
            FROM message_reservations
            WHERE status_id = ?
              AND scheduled_at <= NOW()
            """,
        Long.class,
        waitingStatusId
    );

    if (candidateIds.isEmpty()) {
      return RepeatStatus.FINISHED;
    }

    // 조건부 선점
    List<Long> lockedIds = new ArrayList<>();

    for (Long reservationId : candidateIds) {
      int updated = jdbcTemplate.update(
          """
              UPDATE message_reservations
              SET status_id = ?
              WHERE id = ?
                AND status_id = ?
              """,
          processingStatusId,
          reservationId,
          waitingStatusId
      );

      if (updated == 1) {
        lockedIds.add(reservationId);
      }
    }

    // JobExecutionContext에 저장
    ExecutionContext jobContext =
        chunkContext.getStepContext()
            .getStepExecution()
            .getJobExecution()
            .getExecutionContext();

    jobContext.put("reservationIds", lockedIds);

    return RepeatStatus.FINISHED;
  }
}
