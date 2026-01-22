package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.domain.RecoveredJobTarget;
import java.time.YearMonth;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

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

    String targetMonth = YearMonth.now().minusMonths(1).toString();

    log.warn(">>> 서버 기동 시 배치 복구 시작. targetMonth={}", targetMonth);

    // targetMonth 기준 마지막 STARTED 1건만 복구
    RecoveredJobTarget target =
        batchRecoveryService.recoverLatestExecution(targetMonth);

    if (target == null) {
      log.warn(">>> 복구 대상 배치 없음");
      return;
    }

    try {
      log.warn(">>> 복구된 배치 재시작 시도: {}",
          target.getJobParameters());

      jobLauncher.run(billingJob, target.getJobParameters());

      log.warn(">>> 서버 기동 시 배치 재시작 요청 완료");

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
    }

    log.warn(">>> 서버 기동 시 배치 복구 종료");
  }

}