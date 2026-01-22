package com.example.billingbatch.common.code;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CodeCache {

  private final JdbcTemplate jdbcTemplate;

  /**
   * key: GROUP_CODE:CODE value: codes.id
   */
  private final Map<String, Long> codeIdByKey = new HashMap<>();
  private final Map<Long, String> codeById = new HashMap<>();

  @EventListener(ApplicationReadyEvent.class)
  public void init() {
    String sql = """
          SELECT
            cg.code AS group_code,
            c.code  AS code,
            c.id    AS id
          FROM codes c
          JOIN code_groups cg ON c.group_id = cg.id
        """;

    List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

    for (Map<String, Object> row : rows) {
      String key =
          row.get("group_code") + ":" + row.get("code");
      Long id =
          ((Number) row.get("id")).longValue();
      String code = (String) row.get("code");

      codeIdByKey.put(key, id);
      codeById.put(id, code);
    }
  }

  public Long getId(String groupCode, String code) {
    Long id = codeIdByKey.get(groupCode + ":" + code);
    if (id == null) {
      throw new IllegalStateException(
          "Code not found: " + groupCode + ":" + code
      );
    }
    return id;
  }

  public String getCode(Long id) {
    String code = codeById.get(id);
    if (code == null) {
      throw new IllegalStateException(
          "Code not found for id=" + id
      );
    }
    return code;
  }
}
