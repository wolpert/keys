package com.codeheadsystems.pretender.factory;

import static org.slf4j.LoggerFactory.getLogger;

import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.model.Database;
import com.codeheadsystems.pretender.model.Metadata;
import org.jdbi.v3.cache.caffeine.CaffeineCachePlugin;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;

/**
 * The type Jdbi factory.
 */
public class JdbiFactory {

  private static final Logger log = getLogger(JdbiFactory.class);

  private final Database database;

  /**
   * Instantiates a new Jdbi factory.
   *
   * @param configuration the configuration
   */
  public JdbiFactory(final Configuration configuration) {
    this.database = configuration.database();
    log.info("JdbiFactory()");
  }

  /**
   * Create jdbi jdbi.
   *
   * @return the jdbi
   */
  public Jdbi createJdbi() {
    log.trace("createJdbi()");
    final Jdbi jdbi = Jdbi.create(database.url(), database.username(), database.password());
    setup(jdbi);
    return jdbi;
  }

  private void setup(final Jdbi jdbi) {
    log.info("setup({})", jdbi);
    jdbi.getConfig(JdbiImmutables.class)
        .registerImmutable(Metadata.class);
    jdbi.installPlugin(new SqlObjectPlugin())
        .installPlugin(new CaffeineCachePlugin());
  }
}
