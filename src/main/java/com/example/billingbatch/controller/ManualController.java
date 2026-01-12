package com.example.billingbatch.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/scheduler")
public class ManualController {

  private final JdbcTemplate jdbcTemplate;

  public ManualController(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

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
}
