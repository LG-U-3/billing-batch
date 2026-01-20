package com.example.billingbatch.jobs.settlement;

import com.example.billingbatch.domain.BillingSettlement;
import com.example.billingbatch.domain.ChargedHistory;
import com.example.billingbatch.jobs.settlement.BillingJobListener;
import java.net.ConnectException;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BillingJobConfig {

  private final DataSource dataSource;
  private final BillingJobListener billingJobListener;
  private final SettlementProcessor settlementProcessor;
  private final JdbcTemplate jdbcTemplate;


  int size = 10_000;

  @Bean
  public Job billingJob(JobRepository jobRepository, Step settlementStep, Step messageReservationStep) {
    return new JobBuilder("billingJob", jobRepository)
        .listener(billingJobListener)
        .start(settlementStep)
        .next(messageReservationStep)
        .build();
  }

  // ★ Chunk Size 10_000 설정
  @Bean
  public Step settlementStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("settlementStep", jobRepository)
        .<Long, BillingSettlement>chunk(size, transactionManager)
        .reader(drivingReader())
        .processor(settlementProcessor)
        .writer(settlementWriter())
        .faultTolerant()
        .retry(ConnectException.class)
        .retry(UnsatisfiedDependencyException.class)
        .retryLimit(3)
        .build();
  }

  // ★ Reader: 유저 ID만 페이징으로 가져옴
  @Bean
  public JdbcPagingItemReader<Long> drivingReader() {
    Map<String, Order> sortKeys = new HashMap<>();
    sortKeys.put("user_id", Order.ASCENDING);

    return new JdbcPagingItemReaderBuilder<Long>()
        .name("drivingReader")
        .dataSource(dataSource)
        .pageSize(size)
        .fetchSize(size)
        .selectClause("SELECT DISTINCT user_id")
        .fromClause("FROM charged_histories")
        .sortKeys(sortKeys)
        .rowMapper((rs, rowNum) -> rs.getLong("user_id"))
        .build();
  }

  // ★ Writer: 10000개 모아서 한 방에 Insert
  @Bean
  public JdbcBatchItemWriter<BillingSettlement> settlementWriter() {
    return new JdbcBatchItemWriterBuilder<BillingSettlement>()
        .dataSource(dataSource)
        .sql("INSERT INTO billing_settlements (batch_run_id, user_id, target_month, detail_json, final_amount) " +
            "VALUES (:batchRunId, :userId, :targetMonth, :detailJson, :finalAmount)")
        .beanMapped()
        .build();
  }

  // Step 2 (Tasklet)은 기존 유지...
  // =========================================================================
  // 3. Step 2: 알림 예약 (Tasklet - 단일 건 처리)
  // =========================================================================
  @Bean
  public Step messageReservationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("messageReservationStep", jobRepository)
        .tasklet((contribution, chunkContext) -> {

//          // 예시: 전체 유저 그룹(group_id=1)에게 내일 오전 9시 발송 예약
//          // template_id 등은 실제 DB에 존재하는 값이어야 함
//          String sql = "INSERT INTO message_reservations " +
//              "(status, scheduled_at, channel_type, template_id, template_type, user_group_id) " +
//              "VALUES (?, ?, ?, ?, ?, ?)";
//
//          jdbcTemplate.update(sql,
//              "WAITING",
//              LocalDateTime.now().plusDays(1).withHour(9).withMinute(0).withSecond(0), // 내일 09:00
//              "SMS",
//              1L, // 템플릿 ID (Dummy)
//              "SMS",
//              1L  // 전체 유저 그룹 ID (Dummy)
//          );

          log.info(">>> [알림 예약] 정산 완료 알림 메시지 예약 생성 완료");
          return RepeatStatus.FINISHED;
        }, transactionManager)
        .build();
  }
}
