package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.domain.RecoveredJobTarget;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/** Job 재시작 제어 **/

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchRecoveryService {

  private final JobExplorer jobExplorer;
  private final JobRepository jobRepository;

  private final JdbcTemplate jdbcTemplate;


  /** 공통로직 **/
  private void recoverExecution(JobExecution execution) {

    log.warn(">>> [배치 복구] execution 복구 시작. id={}, status={}",
        execution.getId(), execution.getStatus());

    /** batch_runs에 데이터 넣기 **/
    // 1. StepExecution 집계
    long successCount = 0;
    long failCount = 0;

    for (StepExecution step : execution.getStepExecutions()) {
      successCount += step.getWriteCount();

      failCount += step.getReadSkipCount()
          + step.getProcessSkipCount()
          + step.getWriteSkipCount();
    }

    long totalCount = successCount + failCount;

    Long batchRunId = execution.getId();

    log.warn("배치런아이디 확인 >>>>>>!!!! batchRunId ={}", batchRunId);

    // 2. batch_runs 업데이트
    if (batchRunId != null) {
      jdbcTemplate.update("""
      UPDATE batch_runs
         SET status_id = 5,          -- FAILED
             total_count = ?,
             success_count = ?,
             fail_count = ?,
             fail_reason = 'SERVER_CRASH'
       WHERE batch_run_id = ?
    """,
          totalCount,
          successCount,
          failCount,
          batchRunId
      );
    }


    // StepExecution 정리
    for (StepExecution stepExecution : execution.getStepExecutions()) {
      if (stepExecution.getStatus() == BatchStatus.STARTED
          || stepExecution.getStatus() == BatchStatus.STOPPING) {

        stepExecution.setStatus(BatchStatus.FAILED);
        stepExecution.setExitStatus(ExitStatus.FAILED);

        jobRepository.update(stepExecution);
      }
    }

    // JobExecution 정리
    execution.setStatus(BatchStatus.FAILED);
    execution.setExitStatus(ExitStatus.FAILED);

    jobRepository.update(execution);

    log.warn(">>> [배치 복구] executionId={} → FAILED 처리 완료",
        execution.getId());
  }



  /** 서버 시작 시 바로 실행, 재실행 위한 값(job name, job parameter) return **/
  public List<RecoveredJobTarget> recoverAllStuckExecutions() {

    Set<JobExecution> stuckExecutions =
        jobExplorer.findRunningJobExecutions("billingJob");

    List<RecoveredJobTarget> recoveredTargets = new ArrayList<>();

    for (JobExecution execution : stuckExecutions) {

      recoverExecution(execution); // STARTED → FAILED

      recoveredTargets.add(
          new RecoveredJobTarget(
              execution.getJobInstance().getJobName(),
              execution.getJobParameters()
          )
      );
    }

    return recoveredTargets;
  }

}