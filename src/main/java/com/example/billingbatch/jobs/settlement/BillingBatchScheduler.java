package com.example.billingbatch.jobs.settlement;
// TODO 1/16 스케줄러 수정 예정

import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BillingBatchScheduler {

  private final JobLauncher jobLauncher;
  private final Job billingJob; // 설정한 Job의 Bean 이름

  // 매달 1일 새벽 2시에 실행 (Cron 표현식)
  @Scheduled(cron = "0 0 2 1 * *", zone = "Asia/Seoul") // 초 분 시 일 월 년
  public void runBillingJob() {
    try {
      JobParameters jobParameters = new JobParametersBuilder()
          .addString("requestDate", LocalDateTime.now().toString())
          .toJobParameters();

      jobLauncher.run(billingJob, jobParameters);
    } catch (Exception e) {
      log.error("정산 배치 실행 중 에러 발생: {}", e.getMessage());
    }
  }
}
