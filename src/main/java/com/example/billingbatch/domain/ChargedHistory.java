package com.example.billingbatch.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChargedHistory {
  private Long id;
  private Long userId;
  private Long serviceId;
  private LocalDateTime createdAt;
  private Long chargedPrice;
  private Long contractDiscountPrice; // Nullable
  private Long bundledDiscountPrice;  // Nullable
  private Long premierDiscountPrice;  // Nullable
}