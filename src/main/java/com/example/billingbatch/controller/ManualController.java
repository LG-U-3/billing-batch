package com.example.billingbatch.controller;

import com.example.billingbatch.jobs.settlement.BatchRecoveryService;
import java.sql.SQLTransientConnectionException;
import java.time.YearMonth;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.CannotCreateTransactionException;
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

  /**
   * 상태 변경
   **/
  private final BatchRecoveryService batchRecoveryService;

  @PostMapping("/manual")
  public String insertManualRun(
      @RequestParam(defaultValue = "tester") String triggeredBy
  ) {
    jdbcTemplate.update(
        "INSERT INTO scheduler_test_runs (run_type, triggered_by, created_at) VALUES (?, ?, NOW())",
        "MANUAL",
        triggeredBy
    );

    return "MANUAL scheduler_test_runs inserted";
  }

  @PostMapping("/billing-job")
  public String launchBillingJob() {

    String targetMonth = YearMonth.now().minusMonths(1).toString();

    log.info(">>>>> [배치API호출됨] billing job for month: {}", targetMonth);

    JobParameters jobParameters = new JobParametersBuilder()
        // JobInstance 식별자 (고정)
        .addString("targetMonth", targetMonth)
        .addString("createdBy", "ADMIN", false)
        .toJobParameters();


    try {
      jobLauncher.run(billingJob, jobParameters);
      return ">>>>> 배치 실행 요청 완료";
    } catch (JobExecutionAlreadyRunningException e) { // 이미 실행중인 배치 있을 때: 같은 JobName + 같은 JobParameter + 상태 started
      return ">>>>> 이미 실행 중인 배치가 있습니다.";
    } catch (JobInstanceAlreadyCompleteException e) { // 이미 완료된 배치일 때: 같은 JobName + 같은 JobParameter + 상태 completed
      return ">>>>> 이미 완료된 배치입니다.";
    } catch (UnsatisfiedDependencyException
             | CannotCreateTransactionException e) {
      log.error(">>> db 연결에 오류가 발생했습니다.", e);
      return ">>>>> DB 연결에 문제가 발생했습니다. 연결을 확인해주세요.";
    }catch (Exception e) {
      log.error(">>>>> 배치 실행 중 에러", e);
      return ">>>>> 배치 실행 실패";
    }
  }

  /** 자동 배치 completed 이후 수동 재실행 **/
  @PostMapping("/billing-job-retry")
  public String launchBillingJobTest() {

    String targetMonth = YearMonth.now().minusMonths(1).toString();

    log.info(">>>>> [배치RETRY-API호출됨] billing job for month: {}, retry ", targetMonth);

    /** 재실행 전 기존 실행 기록 삭제 **/

    // billing_settlements 삭제
    jdbcTemplate.update(
        "DELETE FROM billing_settlements WHERE target_month = ?",
        targetMonth
    );

    // batch_runs 삭제
    jdbcTemplate.update(
        "DELETE FROM batch_runs WHERE target_month = ?",
        targetMonth
    );

    JobParameters jobParameters = new JobParametersBuilder()
        // JobInstance 식별자 (고정)
        .addString("targetMonth", targetMonth)
        .addLong("runAt", System.currentTimeMillis())
        .addString("createdBy", "ADMIN", false)
        .toJobParameters();


    try {
      jobLauncher.run(billingJob, jobParameters);
      return ">>>>> 배치 실행 요청 완료";
    } catch (JobExecutionAlreadyRunningException e) { // 이미 실행중인 배치 있을 때: 같은 JobName + 같은 JobParameter + 상태 started
      return ">>>>> 이미 실행 중인 배치가 있습니다.";
    } catch (JobInstanceAlreadyCompleteException e) { // 이미 완료된 배치일 때: 같은 JobName + 같은 JobParameter + 상태 completed
      return ">>>>> 이미 완료된 배치입니다.";
    } catch (UnsatisfiedDependencyException | CannotCreateTransactionException e) {
      return ">>>>> DB 연결에 문제가 발생했습니다. 연결을 확인해주세요.";
    } catch (Exception e) {
      log.error(">>>>> 배치 실행 중 에러", e);
      return ">>>>> 배치 실행 실패";
    }
  }

}

