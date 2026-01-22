package com.example.billingbatch.jobs.reservation.step;

import com.example.billingbatch.jobs.reservation.step.tasklet.ReservationPickTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ReservationPickStepConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final ReservationPickTasklet reservationPickTasklet;

  @Bean
  public Step reservationPickStep() {
    return new StepBuilder("reservationPickStep", jobRepository)
        .tasklet(reservationPickTasklet, transactionManager)
        .build();
  }
}
