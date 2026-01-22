package com.example.billingbatch.jobs.reservation.step;

import com.example.billingbatch.domain.MessageSendResult;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class MessageResultCreateStepConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Step messageResultCreateStep(
      ItemReader<Long> userIdItemReader,
      ItemProcessor<Long, MessageSendResult> messageResultItemProcessor,
      ItemWriter<MessageSendResult> messageResultItemWriter
  ) {
    return new StepBuilder("messageResultCreateStep", jobRepository)
        .<Long, MessageSendResult>chunk(10_000, transactionManager)
        .reader(userIdItemReader)
        .processor(messageResultItemProcessor)
        .writer(messageResultItemWriter)
        .build();
  }
}
