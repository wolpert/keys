package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.dao.PdbTableDao;
import com.codeheadsystems.pretender.factory.JdbiFactory;
import com.codeheadsystems.pretender.liquibase.LiquibaseHelper;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;

/**
 * The type Pretender module.
 */
@Module(includes = PretenderModule.Binder.class)
public class PretenderModule {

  /**
   * The constant LIQUIBASE_SETUP_XML.
   */
  public static final String LIQUIBASE_SETUP_XML = "liquibase/liquibase-setup.xml";

  /**
   * Jebi jdbi.
   *
   * @param factory         the factory
   * @param liquibaseHelper the liquibase helper
   * @return the jdbi
   */
  @Provides
  @Singleton
  public Jdbi jebi(final JdbiFactory factory,
                   final LiquibaseHelper liquibaseHelper) {
    final Jdbi jdbi = factory.createJdbi();
    liquibaseHelper.runLiquibase(jdbi, LIQUIBASE_SETUP_XML);
    return jdbi;
  }

  /**
   * PdbTable dao metadata dao.
   *
   * @param jdbi the jdbi
   * @return the metadata dao
   */
  @Provides
  @Singleton
  public PdbTableDao metadataDao(final Jdbi jdbi) {
    return jdbi.onDemand(PdbTableDao.class);
  }

  /**
   * The interface Binder.
   */
  @Module
  interface Binder {

  }
}
