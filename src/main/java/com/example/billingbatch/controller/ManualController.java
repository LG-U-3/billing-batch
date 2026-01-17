package com.example.billingbatch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/scheduler")
@RequiredArgsConstructor
public class ManualController {

  private final JdbcTemplate jdbcTemplate;
  private final JobLauncher jobLauncher;
  private final Job billingJob;

  @PostMapping("/billing-job")
  public String launchBillingJob(@RequestParam(defaultValue = "2025-12") String targetMonth) {
    log.info(">>>>> [배치API호출됨] Received a request to run the billing job for month: {}", targetMonth);
    System.out.println("=== JobController START ==");
    Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM billing_settlements", Integer.class);

    if (count != null && count == 0) {
      log.info(">>>>> [배치 실행] billing_settlements 테이블이 비어있습니다. 배치를 실행합니다.");

      JobParameters jobParameters = new JobParametersBuilder()
          .addString("targetMonth", "2025-12")
          .addLong("time", System.currentTimeMillis())
          .toJobParameters();

      try {
        jobLauncher.run(billingJob, jobParameters);
        return ">>>>> 배치가 성공적으로 완료되었습니다.";
      } catch (Exception e) {
        log.error(">>>>> [에러발생] 배치 실행 중 에러 발생: {}", e.getMessage());
        e.printStackTrace();
        return ">>>>> 오류가 발생하여 종료합니다.";
      }
    } else {
      return ">>>>> billing_settlements 데이터가 존재합니다. 개수 : " + count + " 개 | 배치를 실행하지 않습니다.";
    }
  }

}
