package com.codeheadsystems.pretender.factory;

import static org.slf4j.LoggerFactory.getLogger;

import com.codeheadsystems.pretender.liquibase.LiquibaseHelper;
import com.codeheadsystems.pretender.model.Database;
import com.codeheadsystems.pretender.model.PdbTable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.cache.caffeine.CaffeineCachePlugin;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.core.statement.Slf4JSqlLogger;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;

/**
 * The type Jdbi factory.
 */
@Singleton
public class JdbiFactory {

  private static final Logger log = getLogger(JdbiFactory.class);

  private final Database database;

  /**
   * Instantiates a new Jdbi factory.
   *
   * @param database the configuration
   */
  @Inject
  public JdbiFactory(final Database database) {
    this.database = database;
    log.info("JdbiFactory({})", database);
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
        .registerImmutable(PdbTable.class);
    jdbi.installPlugin(new SqlObjectPlugin())
        .installPlugin(new CaffeineCachePlugin());
    if (database.usePostgresql()) {
      jdbi.installPlugin(new PostgresPlugin());
    }
    jdbi.setSqlLogger(new Slf4JSqlLogger());
  }
}
