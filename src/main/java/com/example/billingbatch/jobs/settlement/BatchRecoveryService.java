package com.example.billingbatch.jobs.settlement;

import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.stereotype.Service;

/** Job 실행/중단/재시작 제어 **/

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchRecoveryService {

  private final JobExplorer jobExplorer;
  private final JobRepository jobRepository;

  /**
   * STARTED 상태로 남아 있는 JobExecution을 안전하게 중단
   */
  public void recoverForRestart(String jobName, JobParameters jobParameters) {

    JobInstance jobInstance =
        jobExplorer.getJobInstance(jobName, jobParameters);

    if (jobInstance == null) {
      log.info(">>> [배치 복구] JobInstance 없음. 복구할 대상 없음");
      return;
    }

    for (JobExecution execution : jobExplorer.getJobExecutions(jobInstance)) {

      if (execution.getStatus() == BatchStatus.STARTED
          || execution.getStatus() == BatchStatus.STOPPING) {

        log.warn(">>> [배치 복구] 재시작 대상 execution 발견. id={}, status={}",
            execution.getId(), execution.getStatus());

        // 1.  StepExecution 먼저 정리
        for (StepExecution stepExecution : execution.getStepExecutions()) {
          if (stepExecution.getStatus().isRunning()) {
            stepExecution.setStatus(BatchStatus.FAILED);
            stepExecution.setExitStatus(ExitStatus.FAILED);
            stepExecution.setEndTime(LocalDateTime.now());

            jobRepository.update(stepExecution);

            log.warn(">>> [배치 복구] StepExecution {} → FAILED",
                stepExecution.getId());
          }
        }

        // 2. JobExecution 정리
        execution.setStatus(BatchStatus.FAILED);
        execution.setExitStatus(ExitStatus.FAILED);
        execution.setEndTime(LocalDateTime.now());

        jobRepository.update(execution);

        log.warn(">>> [배치 복구] executionId={} → FAILED 처리 완료",
            execution.getId());

      }
    }
  }
}