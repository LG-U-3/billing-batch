package com.example.billingbatch.jobs.reservation.job;

import com.example.billingbatch.common.code.CodeCache;
import com.example.billingbatch.jobs.reservation.partition.ReservationPartitioner;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@RequiredArgsConstructor
public class ReservedMessageJobConfig {

  private final JobRepository jobRepository;
  private final JdbcTemplate jdbcTemplate;
  private final CodeCache codeCache;

  // 외부 StepConfig 에서 생성된 Step들만 주입
  private final Step reservationPickStep;
  private final Step messageResultCreateStep;
  private final Step reservationFinalizeStep;

  /**
   * Job 정의
   */
  @Bean
  public Job reservedMessageJob() {
    return new JobBuilder("reservedMessageJob", jobRepository)
        .start(reservationPickStep)
        .next(reservationPartitionStep())
        .build();
  }

  /**
   * Partition Step
   */
  @Bean
  public Step reservationPartitionStep() {
    return new StepBuilder("reservationPartitionStep", jobRepository)
        .partitioner("reservationSlaveStep", reservationPartitioner(jdbcTemplate, codeCache))
        .step(reservationSlaveStep())
        .taskExecutor(reservationTaskExecutor())
        .build();
  }

  /**
   * reservation 1건 처리 단위 Step
   */
  @Bean
  public Step reservationSlaveStep() {
    return new StepBuilder("reservationSlaveStep", jobRepository)
        .flow(reservationSlaveFlow())
        .build();
  }

  /**
   * reservation 단위 Flow
   */
  @Bean
  public Flow reservationSlaveFlow() {
    return new FlowBuilder<Flow>("reservationSlaveFlow")
        .start(messageResultCreateStep)
        .next(reservationFinalizeStep)
        .build();
  }

  /**
   * Partitioner
   */
  @Bean
  public Partitioner reservationPartitioner(
      JdbcTemplate jdbcTemplate,
      CodeCache codeCache
  ) {
    return new ReservationPartitioner(jdbcTemplate, codeCache);
  }

  /**
   * Partition용 TaskExecutor
   */
  @Bean
  public TaskExecutor reservationTaskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(4);
    executor.setMaxPoolSize(8);
    executor.setThreadNamePrefix("reservation-partition-");
    executor.initialize();
    return executor;
  }
}
