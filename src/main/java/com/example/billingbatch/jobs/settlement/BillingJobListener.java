package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.jobs.settlement.status.BatchRunStatus;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BillingJobListener implements JobExecutionListener {

  private final JdbcTemplate jdbcTemplate;

  @Override
  public void beforeJob(JobExecution jobExecution) {
    // 1. Job Parameter에서 대상 월 가져오기 (없으면 이번 달)
    String targetMonth = jobExecution.getJobParameters().getString("targetMonth", "2025-12");

    String checkSql = "SELECT COUNT(*) FROM batch_runs WHERE target_month = ? AND status_id = 5";
    Integer completedCount = jdbcTemplate.queryForObject(checkSql, Integer.class, targetMonth);

    if (completedCount != null && completedCount > 0) {
      log.warn(">>> [중단] {} 대상월은 이미 정산 완료되었습니다.", targetMonth);
      // 상태를 STOPPED로 설정하고 예외를 던져 실행을 중단합니다.
      jobExecution.setStatus(org.springframework.batch.core.BatchStatus.STOPPED);
      jobExecution.setExitStatus(
          org.springframework.batch.core.ExitStatus.STOPPED.addExitDescription("이미 완료된 배치입니다."));
      throw new RuntimeException("중복 실행 방지: 이미 완료된 정산 대상월입니다.");
    }

    // 2. batch_runs 테이블에 시작 기록 (INSERT)
//    String sql = "INSERT INTO batch_runs (job_code, target_month, batch_status, started_at) VALUES (?, ?, ?, ?)";
    String sql = "INSERT INTO batch_runs (target_month, status_id, started_at) VALUES (?, ?, ?)";

    try {
      KeyHolder keyHolder = new GeneratedKeyHolder();

      jdbcTemplate.update(con -> {
        PreparedStatement ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, targetMonth);
        ps.setLong(2, 4);
        ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        return ps;
      }, keyHolder);

      // 3. 생성된 PK(batch_run_id)를 JobExecution Context에 저장 (Processor 등에서 쓰기 위함)
      Long batchRunId = keyHolder.getKey().longValue();
      jobExecution.getExecutionContext().putLong("batchRunId", batchRunId);

      log.info(">>> [배치 시작] ID: {}, 대상월: {}", batchRunId, targetMonth);

    } catch (org.springframework.dao.DuplicateKeyException e) {
      // DB의 Unique Key 제약 조건에 의해 중복 실행이 물리적으로 차단됨
      log.error(">>> [중복 실행 방지] 이미 {} 대상월에 대한 배치가 실행 중입니다.", targetMonth);
      jobExecution.setStatus(org.springframework.batch.core.BatchStatus.FAILED);
      throw new RuntimeException("중복 배치 실행 시도로 인해 강제 종료합니다.");
    }

  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    // 1. Context에서 ID 꺼내기
    Long batchRunId = jobExecution.getExecutionContext().getLong("batchRunId");

    // 2. 성공/실패 건수 집계
//    long writeCount = jobExecution.getStepExecutions().stream().mapToLong(s -> s.getWriteCount()).sum();
//    long failCount = jobExecution.getStepExecutions().stream().mapToLong(s -> s.getSkipCount()).sum();
    long writeCount = jobExecution.getStepExecutions()
        .stream()
        .mapToLong(s -> s.getWriteCount())
        .sum();
    long failCount = jobExecution.getStepExecutions()
        .stream()
        .mapToLong(s -> s.getSkipCount())
        .sum();

    // 3. 상태 결정
//    String status = (jobExecution.getStatus() == BatchStatus.COMPLETED) ? "COMPLETED" : "FAILED";

    // status_id 추가
    long statusId = (jobExecution.getStatus() == BatchStatus.COMPLETED)
        ? BatchRunStatus.COMPLETED.getCodeId()
        : BatchRunStatus.FAILED.getCodeId();

    // 4. 종료 기록 (UPDATE)
    String sql = "UPDATE batch_runs SET status_id = ?, ended_at = ?, success_count = ?, total_count = ? WHERE batch_run_id = ?";

    jdbcTemplate.update(sql, statusId, Timestamp.valueOf(LocalDateTime.now()), writeCount,
        writeCount + failCount, batchRunId);

    log.info(">>> [배치 종료] 상태: {}, 처리건수: {}", statusId, writeCount);
  }
}
