package com.codeheadsystems.pretender.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.codeheadsystems.pretender.BaseJdbiTest;
import com.codeheadsystems.pretender.model.ImmutablePdbTable;
import com.codeheadsystems.pretender.model.PdbTable;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PdbTableDaoTest extends BaseJdbiTest {

  private static final PdbTable PDB_TABLE = ImmutablePdbTable.builder()
      .name("name")
      .hashKey("hashKey")
      .sortKey("sortKey")
      .createDate(Instant.now().truncatedTo(ChronoUnit.MILLIS))
      .build();

  private PdbTableDao pdbTableDao;

  @BeforeEach
  void setup() {
    pdbTableDao = jdbi.onDemand(PdbTableDao.class);
  }

  @Test
  void testRoundTrip() {
    assertThat(pdbTableDao.listTableNames()).isEmpty();
    assertThat(pdbTableDao.getTable(PDB_TABLE.name())).isEmpty();
    assertThat(pdbTableDao.insert(PDB_TABLE)).isEqualTo(true);
    assertThatExceptionOfType(UnableToExecuteStatementException.class)
        .isThrownBy(() -> pdbTableDao.insert(PDB_TABLE))
        .withCauseInstanceOf(SQLIntegrityConstraintViolationException.class);
    assertThat(pdbTableDao.listTableNames()).containsExactly(PDB_TABLE.name());
    assertThat(pdbTableDao.getTable(PDB_TABLE.name())).contains(PDB_TABLE);
    assertThat(pdbTableDao.delete(PDB_TABLE.name())).isEqualTo(true);
    assertThat(pdbTableDao.delete(PDB_TABLE.name())).isEqualTo(false);
    assertThat(pdbTableDao.listTableNames()).isEmpty();
    assertThat(pdbTableDao.getTable(PDB_TABLE.name())).isEmpty();
  }

}