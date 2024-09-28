package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.pretender.dao.PdbTableDao;
import com.codeheadsystems.pretender.model.PdbTable;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type PdbTable manager.
 */
@Singleton
public class PdbTableManager {

  private static final Logger log = LoggerFactory.getLogger(PdbTableManager.class);

  private final PdbTableDao pdbTableDao;

  /**
   * Instantiates a new PdbTable manager.
   *
   * @param pdbTableDao the dao
   */
  @Inject
  public PdbTableManager(final PdbTableDao pdbTableDao) {
    log.info("PdbTableManager({})", pdbTableDao);
    this.pdbTableDao = pdbTableDao;
  }

  /**
   * Insert pdb table boolean.
   *
   * @param pdbTable the pdb table
   * @return the boolean
   */
  public boolean insertPdbTable(final PdbTable pdbTable) {
    log.trace("insertPdbTable({})", pdbTable);
    if (getPdbTable(pdbTable.name()).isPresent()) {
      log.warn("Table already exists: {}", pdbTable);
      return false;
    }
    try {
      return pdbTableDao.insert(pdbTable);
    } catch (UnableToExecuteStatementException e) {
      if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
        log.warn("Table already exists: {}", pdbTable);
        return false;
      } else {
        log.error("Unable to insert table: {}", pdbTable, e);
        throw e;
      }
    }
  }

  /**
   * Gets pdb table.
   *
   * @param name the name
   * @return the pdb table
   */
  public Optional<PdbTable> getPdbTable(final String name) {
    log.trace("getPdbTable({})", name);
    return pdbTableDao.getTable(name);
  }

  /**
   * Delete pdb table boolean.
   *
   * @param name the name
   * @return the boolean
   */
  public boolean deletePdbTable(final String name) {
    log.trace("deletePdbTable({})", name);
    return pdbTableDao.delete(name);
  }

  /**
   * List pdb tables list.
   *
   * @return the list
   */
  public List<String> listPdbTables() {
    log.trace("listPdbTables()");
    return pdbTableDao.listTableNames();
  }
}
