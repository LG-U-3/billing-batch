package com.example.billingbatch.jobs.settlement.status;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum BatchRunStatus {
  RUNNING(4L),
  COMPLETED(5L),
  FAILED(6L);

  private final long codeId;
}