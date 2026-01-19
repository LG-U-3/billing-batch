package com.example.billingbatch.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.batch.core.JobParameters;

@Getter
@AllArgsConstructor
public class RecoveredJobTarget {

  private final String jobName;
  private final JobParameters jobParameters;
}