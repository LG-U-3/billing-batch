package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.domain.RecoveredJobTarget;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

/** 서버 시작 후 바로 실행 **/
@Component
@RequiredArgsConstructor
@Slf4j
public class BatchStartupRecovery {

  private final BatchRecoveryService batchRecoveryService;

  private final JobLauncher jobLauncher;
  private final Job billingJob;

  /** 상태 변경 후 재시작까지 **/
  @EventListener(ApplicationReadyEvent.class)
  public void recoverAndRestartIfNeeded() {

    log.warn(">>> 서버 기동 시 배치 복구 시작");

    List<RecoveredJobTarget> recovered =
        batchRecoveryService.recoverAllStuckExecutions();

    for (RecoveredJobTarget target : recovered) {

      try {
        log.warn(">>> 복구된 배치 재실행 시도: {}",
            target.getJobParameters());

        jobLauncher.run(billingJob, target.getJobParameters());

      } catch (JobExecutionAlreadyRunningException e) {
        log.warn(">>> 이미 실행 중인 배치입니다. params={}",
            target.getJobParameters());

      } catch (JobInstanceAlreadyCompleteException e) {
        log.warn(">>> 이미 완료된 배치입니다. params={}",
            target.getJobParameters());

      } catch (JobRestartException e) {
        log.error(">>> 배치 재시작 불가 상태입니다. params={}",
            target.getJobParameters(), e);

      } catch (JobParametersInvalidException e) {
        log.error(">>> 잘못된 JobParameters입니다. params={}",
            target.getJobParameters(), e);
      } catch (UnsatisfiedDependencyException
               | CannotCreateTransactionException e) {
        log.error(">>> db 연결에 오류가 발생했습니다. params={}",
            target.getJobParameters(), e);
      }
    }

    log.warn(">>> 서버 기동 시 배치 복구 완료");
  }

}