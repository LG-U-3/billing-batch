package com.example.billingbatch.jobs.reservation.step;

import com.example.billingbatch.jobs.reservation.step.tasklet.ReservationFinalizeTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ReservationFinalizeStepConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final ReservationFinalizeTasklet reservationFinalizeTasklet;

  @Bean
  public Step reservationFinalizeStep() {
    return new StepBuilder("reservationFinalizeStep", jobRepository)
        .tasklet(reservationFinalizeTasklet, transactionManager)
        .listener(reservationFinalizeTasklet)
        .build();
  }
}