package com.example.billingbatch.recovery.dao;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class MessageSendResultJdbcDao {

  private final NamedParameterJdbcTemplate jdbc;

  /** PROCESSING & requested_at <= threshold 인 id 목록 (limit 적용) */
  public List<Long> findProcessingTimeoutIds(Long processingStatusId, LocalDateTime threshold,
      int limit) {
    String sql = """
        SELECT msr.id
        FROM message_send_results msr
        WHERE msr.status_id = :processingStatusId
          AND msr.requested_at <= :threshold
        ORDER BY msr.id
        LIMIT :limit
        """;

    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("processingStatusId", processingStatusId)
            .addValue("threshold", Timestamp.valueOf(threshold)).addValue("limit", limit);

    return jdbc.query(sql, params, (rs, rowNum) -> rs.getLong("id"));
  }

  /** PROCESSING -> FAILED 벌크 업데이트 */
  public int markFailedByIds(List<Long> ids, Long processingStatusId, Long failedStatusId) {
    if (ids == null || ids.isEmpty())
      return 0;

    String sql = """
        UPDATE message_send_results
        SET status_id = :failedStatusId
        WHERE id IN (:ids)
          AND status_id = :processingStatusId
        """;

    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("failedStatusId", failedStatusId)
            .addValue("processingStatusId", processingStatusId).addValue("ids", ids);

    return jdbc.update(sql, params);
  }

  /** msrId의 현재 status code 조회 (codes.code) */
  public String findStatusCodeByMsrId(Long msrId) {
    String sql = """
        SELECT c.code
        FROM message_send_results msr
        JOIN codes c ON c.id = msr.status_id
        WHERE msr.id = :msrId
        """;

    MapSqlParameterSource params = new MapSqlParameterSource().addValue("msrId", msrId);

    List<String> list = jdbc.query(sql, params, (rs, rowNum) -> rs.getString("code"));
    return list.isEmpty() ? null : list.get(0);
  }

  /** PROCESSING -> FAILED 단건 확정 + processed_at 세팅 */
  public int markFailedOne(Long msrId, Long processingStatusId, Long failedStatusId) {
    String sql = """
        UPDATE message_send_results
        SET status_id = :failedStatusId,
            processed_at = NOW()
        WHERE id = :msrId
          AND status_id = :processingStatusId
          AND processed_at IS NULL
        """;

    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("failedStatusId", failedStatusId)
            .addValue("processingStatusId", processingStatusId).addValue("msrId", msrId);

    return jdbc.update(sql, params);
  }
}
