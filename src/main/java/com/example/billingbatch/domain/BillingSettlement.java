package com.example.billingbatch.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BillingSettlement {
  private Long id;
  private Long batchRunId;    // batch_runs 테이블의 PK
  private String targetMonth; // YYYY-MM
  private Long userId;
  private String detailJson;  // JSON String
  private Long finalAmount;
}