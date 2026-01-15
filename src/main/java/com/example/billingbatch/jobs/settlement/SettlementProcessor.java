package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.domain.BillingSettlement;
import com.example.billingbatch.domain.ChargedHistory;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class SettlementProcessor implements ItemProcessor<Long, BillingSettlement> {

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  private Long batchRunId;
  private String targetMonth;

  @BeforeStep
  public void beforeStep(StepExecution stepExecution) {
    this.batchRunId = stepExecution.getJobExecution().getExecutionContext().getLong("batchRunId");
    this.targetMonth = stepExecution.getJobParameters().getString("targetMonth", "2025-12");
  }

  @Override
  public BillingSettlement process(Long userId) throws Exception {

    String sql = "SELECT * FROM charged_histories WHERE user_id = ?";
    List<ChargedHistory> histories = jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(ChargedHistory.class), userId);

    if (histories.isEmpty()) return null;

    long chargedSum = 0;
    long discountSum = 0;
    List<Map<String, Object>> items = new ArrayList<>();

    for (ChargedHistory h : histories) {
      chargedSum += h.getChargedPrice();

      // Null Safe 처리 (Java 버전에 따라 Objects.requireNonNullElse 등 사용 가능)
      long contract = h.getContractDiscountPrice() != null ? h.getContractDiscountPrice() : 0;
      long bundled = h.getBundledDiscountPrice() != null ? h.getBundledDiscountPrice() : 0;
      long premier = h.getPremierDiscountPrice() != null ? h.getPremierDiscountPrice() : 0;

      discountSum += (contract + bundled + premier);

      Map<String, Object> item = new LinkedHashMap<>();
      item.put("serviceId", h.getServiceId());
      item.put("chargedPrice", h.getChargedPrice());
      item.put("createdAt", h.getCreatedAt().toString());
      items.add(item);
    }

    long finalAmount = chargedSum - discountSum;
    String detailJson = objectMapper.writeValueAsString(items);

    return BillingSettlement.builder()
        .batchRunId(this.batchRunId)
        .userId(userId)
        .targetMonth(this.targetMonth)
        .finalAmount(finalAmount)
        .detailJson(detailJson)
        .build();
  }
}