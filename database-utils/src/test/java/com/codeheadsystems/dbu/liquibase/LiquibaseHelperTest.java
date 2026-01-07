package com.codeheadsystems.dbu.liquibase;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.dbu.factory.JdbiFactory;
import com.codeheadsystems.dbu.model.Database;
import com.codeheadsystems.dbu.model.ImmutableDatabase;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;

class LiquibaseHelperTest  {

  @Test
  void runLiquibase() {
    Database database = ImmutableDatabase.builder()
        .url("jdbc:hsqldb:mem:" + getClass().getSimpleName() + ":" + UUID.randomUUID())
        .username("SA")
        .password("")
        .build();
    Jdbi jdbi = new JdbiFactory(database, Set.of()).createJdbi();
    new LiquibaseHelper().runLiquibase(jdbi, "keys/liquibase-setup.xml");
    final List<Map<String, Object>> list = jdbi.withHandle(handle -> handle.createQuery("select * from PDB_TABLE").mapToMap().list());
    assertThat(list).isEmpty();
  }

}