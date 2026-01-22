package com.example.billingbatch.jobs.reservation.partition;

import com.example.billingbatch.common.code.CodeCache;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;

@RequiredArgsConstructor
public class ReservationPartitioner implements Partitioner {

  private static final String CONTEXT_KEY = "reservationIds";

  private final JdbcTemplate jdbcTemplate;
  private final CodeCache codeCache;

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, ExecutionContext> partition(int gridSize) {

    // JobExecutionContext는 Partitioner가 실행될 때 이미 바인딩됨
    ExecutionContext jobContext =
        org.springframework.batch.core.scope.context.StepSynchronizationManager
            .getContext()
            .getStepExecution()
            .getJobExecution()
            .getExecutionContext();

    Object value = jobContext.get(CONTEXT_KEY);
    if (!(value instanceof List<?> ids) || ids.isEmpty()) {
      return Map.of();
    }

    List<Long> reservationIds = (List<Long>) ids;
    Map<String, ExecutionContext> partitions = new HashMap<>();

    for (Long reservationId : reservationIds) {

      // 🔹 reservation 메타 1회 조회
      ReservationMeta meta = jdbcTemplate.queryForObject(
          """
              SELECT
                r.channel_type_id,
                t.purpose_type_id
              FROM message_reservations r
              JOIN message_templates t ON r.template_id = t.id
              WHERE r.id = ?
              """,
          (rs, rowNum) -> new ReservationMeta(
              rs.getLong("channel_type_id"),
              rs.getLong("purpose_type_id")
          ),
          reservationId
      );

      ExecutionContext context = new ExecutionContext();
      context.putLong("reservationId", reservationId);
      context.putString(
          "channelCode",
          codeCache.getCode(meta.channelTypeId())
      );
      context.putString(
          "purposeCode",
          codeCache.getCode(meta.purposeTypeId())
      );

      partitions.put("reservation-" + reservationId, context);
    }

    return partitions;
  }

  private record ReservationMeta(
      Long channelTypeId,
      Long purposeTypeId
  ) {

  }
}
