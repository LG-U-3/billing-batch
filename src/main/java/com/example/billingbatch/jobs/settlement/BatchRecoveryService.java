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
import org.springframework.stereotype.Service;

/** Job 재시작 제어 **/

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchRecoveryService {

  private final JobExplorer jobExplorer;
  private final JobRepository jobRepository;


  /** 공통로직 **/
  private void recoverExecution(JobExecution execution) {

    log.warn(">>> [배치 복구] execution 복구 시작. id={}, status={}",
        execution.getId(), execution.getStatus());

    // StepExecution 정리
    for (StepExecution stepExecution : execution.getStepExecutions()) {
      if (stepExecution.getStatus() == BatchStatus.STARTED
          || stepExecution.getStatus() == BatchStatus.STOPPING) {

        stepExecution.setStatus(BatchStatus.FAILED);
        stepExecution.setExitStatus(ExitStatus.FAILED);
        stepExecution.setEndTime(LocalDateTime.now());

        jobRepository.update(stepExecution);
      }
    }

    // JobExecution 정리
    execution.setStatus(BatchStatus.FAILED);
    execution.setExitStatus(ExitStatus.FAILED);
    execution.setEndTime(LocalDateTime.now());

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