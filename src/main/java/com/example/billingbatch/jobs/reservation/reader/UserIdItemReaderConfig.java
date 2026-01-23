package com.example.billingbatch.jobs.reservation.reader;

import java.util.Map;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@RequiredArgsConstructor
public class UserIdItemReaderConfig {

  private final DataSource dataSource;
  private final JdbcTemplate jdbcTemplate;

  @Bean
  @StepScope
  public JdbcPagingItemReader<Long> userIdItemReader(
      @Value("#{stepExecutionContext['reservationId']}") Long reservationId
  ) {

    Long userGroupId = jdbcTemplate.queryForObject(
        "SELECT user_group_id FROM message_reservations WHERE id = ?",
        Long.class,
        reservationId
    );

    JdbcPagingItemReader<Long> reader = new JdbcPagingItemReader<>();
    reader.setDataSource(dataSource);
    reader.setPageSize(10_000);
    reader.setRowMapper((rs, rowNum) -> rs.getLong("user_id"));
    reader.setParameterValues(Map.of("groupId", userGroupId));

    MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();
    provider.setSelectClause("SELECT user_id");
    provider.setFromClause("FROM user_user_groups");
    provider.setWhereClause("WHERE group_id = :groupId");
    provider.setSortKeys(Map.of("user_id", Order.ASCENDING));

    reader.setQueryProvider(provider);

    return reader;
  }
}
