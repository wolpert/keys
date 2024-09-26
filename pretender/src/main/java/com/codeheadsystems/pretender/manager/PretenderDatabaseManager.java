package com.codeheadsystems.pretender.manager;

import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Pretender database manager.
 */
public class PretenderDatabaseManager {

  private static final Logger log = LoggerFactory.getLogger(PretenderDatabaseManager.class);

  private final Jdbi jdbi;

  /**
   * Instantiates a new Pretender database manager.
   *
   * @param jdbi the jdbi
   */
  public PretenderDatabaseManager(final Jdbi jdbi) {
    log.info("PretenderDatabaseManager({})", jdbi);
    this.jdbi = jdbi;
  }

}
