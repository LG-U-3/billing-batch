package com.example.billingbatch.jobs.settlement.step2;

import com.example.billingbatch.jobs.settlement.BillingJobListener;
import com.example.billingbatch.jobs.settlement.SettlementProcessor;
import java.time.LocalDateTime;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class MessageReservationConfig {
  // =========================================================================
  // Step 2: 알림 예약 (Tasklet - 단일 건 처리)
  // =========================================================================

  private final DataSource dataSource;
  private final BillingJobListener billingJobListener;
  private final SettlementProcessor settlementProcessor;
  private final JdbcTemplate jdbcTemplate;

  @Bean
  public Step messageReservationStep(
      JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("messageReservationStep", jobRepository)
        .tasklet((contribution, chunkContext) -> {
          // status_id 조회 (WAITING)
          String statusSql = "SELECT id FROM codes WHERE code = 'WAITING' AND group_id = 3";
          Long statusId = jdbcTemplate.queryForObject(statusSql, Long.class);

          // channel_type_id 조회
          String channelSql = "SELECT id FROM codes WHERE code = ?";
          Long channelTypeId = jdbcTemplate.queryForObject(channelSql, Long.class, "EMAIL");

          // template_id 조회
          String templateSql = "SELECT id FROM message_templates WHERE code = ?";
          Long templateId = jdbcTemplate.queryForObject(templateSql, Long.class, "DEFAULT_BILLING_EMAIL");

          // user_group_id 조회
          String userGroupSql = "SELECT id FROM user_groups WHERE code = ?";
          Long userGroupId = jdbcTemplate.queryForObject(userGroupSql, Long.class, "USER_ALL");

          // job parameter에서 "targetMonth" 가져오기
          String targetMonth = contribution.getStepExecution()
              .getJobParameters()
              .getString("targetMonth");

          // 같은 종류의 reservation이 이미 존재하는지?
          String searchReservationSql = "SELECT COUNT(*) FROM message_reservations WHERE "
              + "template_id = ? AND user_group_id = ? AND target_month = ?";
          Integer sameReservationCount = jdbcTemplate.queryForObject(
              searchReservationSql, Integer.class,
              templateId, userGroupId, targetMonth);

          // 없을 경우 reservation insert
          if (sameReservationCount != null && sameReservationCount == 0) {

            String insertSql = "INSERT INTO message_reservations "
                + "(scheduled_at, status_id,  channel_type_id, template_id, user_group_id, target_month) "
                + "VALUES (?, ?, ?, ?, ?, ?)";

            jdbcTemplate.update(insertSql,
                LocalDateTime.now().plusMinutes(5),
                statusId,
                channelTypeId,
                templateId,
                userGroupId,
                targetMonth
            );

            log.info(">>> [알림 예약] 정산 완료 알림 메시지 예약 생성 완료");
          } else {
            log.info(">>> [알림 예약] 이미 예약된 건이 존재하여 생략합니다.");
          }

          return RepeatStatus.FINISHED;
        }, transactionManager)
        .build();
  }
}
