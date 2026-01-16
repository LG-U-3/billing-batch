package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.domain.BillingSettlement;
import com.example.billingbatch.domain.ChargedHistory;
import com.example.billingbatch.domain.UplusService;
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

  private Map<Long, String> serviceIdName;

  @BeforeStep
  public void beforeStep(StepExecution stepExecution) {
    this.batchRunId = stepExecution.getJobExecution().getExecutionContext().getLong("batchRunId");
    this.targetMonth = stepExecution.getJobParameters().getString("targetMonth", "2025-12");

    // service_name 캐싱 (uplus_services - Map<id, name>)
    String sql = "SELECT id, name FROM uplus_services";
    List<UplusService> services = jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(UplusService.class));
    serviceIdName = new LinkedHashMap<>();
    for (UplusService s : services) {
      serviceIdName.put(s.getId(), s.getName());
    }
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

      // Null Safe 처리
      long contract = h.getContractDiscountPrice() != null ? h.getContractDiscountPrice() : 0;
      long bundled = h.getBundledDiscountPrice() != null ? h.getBundledDiscountPrice() : 0;
      long premier = h.getPremierDiscountPrice() != null ? h.getPremierDiscountPrice() : 0;

      discountSum += (contract + bundled + premier);

      Map<String, Object> item = new LinkedHashMap<>();

      // 상품타입
      if (h.getServiceId() == 1 ) item.put("typeId", "부가서비스");
      else if (h.getServiceId() == 2 ) item.put("typeId: ", "소액결제");
      else item.put("typeId", "요금제");

      // 상품ID
      item.put("serviceId", h.getServiceId());

      // 상품명 (serviceIdName에서 id로 조회)
      String serviceName = serviceIdName.get(h.getServiceId());
      item.put("serviceName", serviceName);

      // 상품 금액, 청구일자
      item.put("chargedPrice", h.getChargedPrice());
      item.put("createdAt", h.getCreatedAt().toString());

      // 할인 항목별 금액
      item.put("contractDiscountPrice", contract);
      item.put("BundledDiscountPrice", bundled);
      item.put("PremierDiscountPrice", premier);

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