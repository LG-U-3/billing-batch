package com.example.billingbatch.scheduler;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SchedulerTestInsertJob {

  private final JdbcTemplate jdbcTemplate;

  public SchedulerTestInsertJob(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Scheduled(
      initialDelay = 0,           // 대기 없이
      fixedRate = 10 * 60 * 1000  // 10분 마다
  )
  public void insertAutoRun() {
    System.out.println("=== Scheduler insertAutoRun START ===");
    jdbcTemplate.update(
        "INSERT INTO scheduler_test_runs (run_type, created_at) VALUES (?, NOW())",
        "AUTO"
    );
  }
}
