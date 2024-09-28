package com.codeheadsystems.pretender.factory;

import static com.codeheadsystems.pretender.dagger.ConfigurationModule.RUN_LIQUIBASE;
import static org.slf4j.LoggerFactory.getLogger;

import com.codeheadsystems.pretender.liquibase.LiquibaseHelper;
import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.model.Database;
import com.codeheadsystems.pretender.model.PdbTable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.jdbi.v3.cache.caffeine.CaffeineCachePlugin;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.core.statement.Slf4JSqlLogger;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;

/**
 * The type Jdbi factory.
 */
@Singleton
public class JdbiFactory {

  private static final Logger log = getLogger(JdbiFactory.class);

  private final Database database;
  private final LiquibaseHelper liquibaseHelper;
  private final boolean runLiquibase;

  /**
   * Instantiates a new Jdbi factory.
   *
   * @param configuration   the configuration
   * @param liquibaseHelper the liquibase helper
   * @param runLiquibase    the run liquibase
   */
  @Inject
  public JdbiFactory(final Configuration configuration,
                     final LiquibaseHelper liquibaseHelper,
                     @Named(RUN_LIQUIBASE) final boolean runLiquibase) {
    this.database = configuration.database();
    this.liquibaseHelper = liquibaseHelper;
    this.runLiquibase = runLiquibase;
    log.info("JdbiFactory({},{},{})", configuration, liquibaseHelper, runLiquibase);
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
    if (runLiquibase) {
      liquibaseHelper.runLiquibase(jdbi);
    }
    return jdbi;
  }

  private void setup(final Jdbi jdbi) {
    log.info("setup({})", jdbi);
    jdbi.getConfig(JdbiImmutables.class)
        .registerImmutable(PdbTable.class);
    jdbi.installPlugin(new SqlObjectPlugin())
        .installPlugin(new CaffeineCachePlugin());
    jdbi.setSqlLogger(new Slf4JSqlLogger());
  }
}
