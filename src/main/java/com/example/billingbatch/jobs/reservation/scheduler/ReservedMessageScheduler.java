package com.example.billingbatch.jobs.reservation.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReservedMessageScheduler {

  private final JobLauncher jobLauncher;
  private final Job reservedMessageJob;
  private final JobExplorer jobExplorer;

  @Scheduled(cron = "5 */5 * * * *")// 5분마다 *분 5초에 실행
  public void runReservedMessageJob() {
    if (jobExplorer.findRunningJobExecutions("reservedMessageJob").size() > 0) {
      log.info("예약 발송 배치가 이미 실행 중이므로 스킵합니다.");
      return;
    }
    try {
      JobParameters params = new JobParametersBuilder()
          .addLong("runAt", System.currentTimeMillis()) // JobInstance 구분용
          .toJobParameters();

      jobLauncher.run(reservedMessageJob, params);

    } catch (Exception e) {
      log.error("예약 발송 배치 실행 중 에러 발생: {}", e.getMessage());
    }
  }
}