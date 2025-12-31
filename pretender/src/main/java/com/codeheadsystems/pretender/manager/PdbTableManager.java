package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.pretender.dao.PdbMetadataDao;
import com.codeheadsystems.pretender.model.PdbMetadata;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type PdbMetadata manager.
 */
@Singleton
public class PdbTableManager {

  private static final Logger log = LoggerFactory.getLogger(PdbTableManager.class);

  private final PdbMetadataDao pdbMetadataDao;
  private final PdbItemTableManager pdbItemTableManager;

  /**
   * Instantiates a new PdbMetadata manager.
   *
   * @param pdbMetadataDao       the dao
   * @param pdbItemTableManager the item table manager
   */
  @Inject
  public PdbTableManager(final PdbMetadataDao pdbMetadataDao,
                         final PdbItemTableManager pdbItemTableManager) {
    log.info("PdbTableManager({}, {})", pdbMetadataDao, pdbItemTableManager);
    this.pdbMetadataDao = pdbMetadataDao;
    this.pdbItemTableManager = pdbItemTableManager;
  }

  /**
   * Insert pdb table boolean.
   *
   * @param pdbMetadata the pdb table
   * @return the boolean
   */
  public boolean insertPdbTable(final PdbMetadata pdbMetadata) {
    log.trace("insertPdbTable({})", pdbMetadata);
    if (getPdbTable(pdbMetadata.name()).isPresent()) {
      log.warn("Table already exists: {}", pdbMetadata);
      return false;
    }
    try {
      final boolean inserted = pdbMetadataDao.insert(pdbMetadata);
      if (inserted) {
        // Create the corresponding item storage table
        pdbItemTableManager.createItemTable(pdbMetadata);
        log.info("Created DynamoDB table and item storage table: {}", pdbMetadata.name());
      }
      return inserted;
    } catch (UnableToExecuteStatementException e) {
      if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
        log.warn("Table already exists: {}", pdbMetadata);
        return false;
      } else {
        log.error("Unable to insert table: {}", pdbMetadata, e);
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
  public Optional<PdbMetadata> getPdbTable(final String name) {
    log.trace("getPdbTable({})", name);
    return pdbMetadataDao.getTable(name);
  }

  /**
   * Delete pdb table boolean.
   *
   * @param name the name
   * @return the boolean
   */
  public boolean deletePdbTable(final String name) {
    log.trace("deletePdbTable({})", name);
    final boolean deleted = pdbMetadataDao.delete(name);
    if (deleted) {
      // Drop the corresponding item storage table
      pdbItemTableManager.dropItemTable(name);
      log.info("Deleted DynamoDB table and item storage table: {}", name);
    }
    return deleted;
  }

  /**
   * List pdb tables list.
   *
   * @return the list
   */
  public List<String> listPdbTables() {
    log.trace("listPdbTables()");
    return pdbMetadataDao.listTableNames();
  }
}
