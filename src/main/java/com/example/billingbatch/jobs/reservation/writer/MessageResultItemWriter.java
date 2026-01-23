package com.example.billingbatch.jobs.reservation.writer;

import com.example.billingbatch.common.code.CodeCache;
import com.example.billingbatch.domain.MessageSendResult;
import com.example.billingbatch.producer.MessageStreamProducer;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
@StepScope
@RequiredArgsConstructor
public class MessageResultItemWriter
    implements ItemWriter<MessageSendResult>, StepExecutionListener {

  private static final int SUB_BATCH_SIZE = 500;

  private final JdbcTemplate jdbcTemplate;
  private final MessageStreamProducer messageStreamProducer;
  private final CodeCache codeCache;

  private String channelCode;
  private String purposeCode;

  @Override
  public void beforeStep(StepExecution stepExecution) {
    ExecutionContext ctx = stepExecution.getExecutionContext();

    this.channelCode = ctx.getString("channelCode");
    this.purposeCode = ctx.getString("purposeCode");
  }

  @Override
  @Transactional
  public void write(Chunk<? extends MessageSendResult> chunk) {

    if (chunk.isEmpty()) {
      return;
    }

    List<? extends MessageSendResult> items = chunk.getItems();

    Long processingStatusId =
        codeCache.getId("MESSAGE_SEND_STATUS", "PROCESSING");

    // Chunk(10,000)를 500건씩 분할 처리
    for (int i = 0; i < items.size(); i += SUB_BATCH_SIZE) {

      List<? extends MessageSendResult> subBatch =
          items.subList(i, Math.min(i + SUB_BATCH_SIZE, items.size()));

      // INSERT + AUTO_INCREMENT id 회수
      List<Long> insertedIds = insertSubBatch(subBatch);

      // Redis Stream publish
      List<Long> publishIds = new ArrayList<>(insertedIds);
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              for (Long messageSendResultId : publishIds) {
                messageStreamProducer.publish(
                    messageSendResultId,
                    channelCode,
                    purposeCode
                );
              }
            }
          }
      );
    }
  }

  /**
   * message_send_results INSERT + id 회수
   */
  private List<Long> insertSubBatch(
      List<? extends MessageSendResult> batch) {

    String sql = """
          INSERT INTO message_send_results
            (reserved_send_id, user_id, template_id, status_id, channel_id)
          VALUES (?, ?, ?, ?, ?)
        """;

    List<Long> ids = new ArrayList<>(batch.size());

    for (MessageSendResult r : batch) {

      KeyHolder keyHolder = new GeneratedKeyHolder();

      jdbcTemplate.update(con -> {
        PreparedStatement ps =
            con.prepareStatement(sql, new String[]{"id"});

        ps.setLong(1, r.getReservedSendId());
        ps.setLong(2, r.getUserId());
        ps.setLong(3, r.getTemplateId());
        ps.setLong(4, r.getStatusId());   // WAITING
        ps.setLong(5, r.getChannelId());

        return ps;
      }, keyHolder);

      Number key = keyHolder.getKey();
      if (key == null) {
        throw new IllegalStateException(
            "Generated key is null for MessageSendResult=" + r
        );
      }

      ids.add(key.longValue());
    }

    return ids;
  }

  /**
   * 상태 WAITING → PROCESSING
   */
  private void updateStatusToProcessing(
      List<Long> ids,
      Long processingStatusId) {

    if (ids.isEmpty()) {
      return;
    }

    String inClause =
        ids.stream()
            .map(String::valueOf)
            .reduce((a, b) -> a + "," + b)
            .orElseThrow();

    jdbcTemplate.update(
        """
            UPDATE message_send_results
            SET status_id = ?
            WHERE id IN (%s)
            """.formatted(inClause),
        processingStatusId
    );
  }
}