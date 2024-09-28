package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.pretender.converter.PdbTableConverter;
import com.codeheadsystems.pretender.dao.PdbTableDao;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Metadata manager.
 */
@Singleton
public class PdbTableManager {

  private static final Logger log = LoggerFactory.getLogger(PdbTableManager.class);

  private final PdbTableDao pdbTableDao;
  private final PdbTableConverter pdbTableConverter;

  /**
   * Instantiates a new Metadata manager.
   *
   * @param jdbi              the jdbi
   * @param pdbTableConverter the meta data converter
   */
  @Inject
  public PdbTableManager(final Jdbi jdbi,
                         final PdbTableConverter pdbTableConverter) {
    this.pdbTableConverter = pdbTableConverter;
    log.info("PdbTableManager({})", jdbi);
    this.pdbTableDao = jdbi.onDemand(PdbTableDao.class);
  }

}
