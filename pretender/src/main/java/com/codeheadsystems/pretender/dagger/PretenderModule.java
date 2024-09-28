package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.dao.PdbTableDao;
import com.codeheadsystems.pretender.factory.JdbiFactory;
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
   * Jebi jdbi.
   *
   * @param factory the factory
   * @return the jdbi
   */
  @Provides
  @Singleton
  public Jdbi jebi(final JdbiFactory factory) {
    return factory.createJdbi();
  }

  /**
   * Metadata dao metadata dao.
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
