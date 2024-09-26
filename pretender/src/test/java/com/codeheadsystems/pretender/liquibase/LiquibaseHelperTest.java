package com.codeheadsystems.pretender.liquibase;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.pretender.BaseJdbiTest;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LiquibaseHelperTest extends BaseJdbiTest {

  @Test
  void runLiquibase() {
    LiquibaseHelper helper = new LiquibaseHelper();
    helper.runLiquibase(jdbi);
    final List<Map<String, Object>> list = jdbi.withHandle(handle -> handle.createQuery("select * from PDB_METADATA").mapToMap().list());
    assertThat(list).isEmpty();
  }

}