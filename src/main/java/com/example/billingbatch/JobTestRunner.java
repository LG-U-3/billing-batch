//package com.example.billingbatch;
//

// 프로그램 실행 시 바로 배치job을 진행하는 파일입니다.

//import java.time.Clock;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.batch.core.Job;
//import org.springframework.batch.core.JobParameters;
//import org.springframework.batch.core.JobParametersBuilder;
//import org.springframework.batch.core.launch.JobLauncher;
//import org.springframework.boot.ApplicationArguments;
//import org.springframework.boot.ApplicationRunner;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.stereotype.Component;
//
//@Slf4j
//@RequiredArgsConstructor
//@Component
//public class JobTestRunner implements ApplicationRunner {
//
//  private final JobLauncher jobLauncher;
//  private final Job billingJob;
//  private final JdbcTemplate jdbcTemplate;
//
//  @Override
//  public void run(ApplicationArguments args) throws Exception {
//    System.out.println("=== JobTestRunner START ==");
//    Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM billing_settlements", Integer.class);
//
//    if (count != null && count == 0) {
//      log.info(">>>>> billing_settlements 테이블이 비어있습니다. 배치를 실행합니다.");
//
//      JobParameters jobParameters = new JobParametersBuilder()
//          .addString("targetMonth", "2025-12")
//          .addLong("time", System.currentTimeMillis())
//          .toJobParameters();
//
//      try {
//        jobLauncher.run(billingJob, jobParameters);
//        log.info(">>>>> 배치가 성공적으로 완료되었습니다.");
//      } catch (Exception e) {
//        log.error(">>>>> 배치 실행 중 에러 발생: {}", e.getMessage());
//        e.printStackTrace();
//      }
//    } else {
//      log.info(">>>>> billing_settlements 데이터가 존재합니다 (count: {}). 배치를 실행하지 않습니다.", count);
//    }
//  }
//}
