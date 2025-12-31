package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.dao.PdbMetadataDao;
import com.codeheadsystems.dbu.factory.JdbiFactory;
import com.codeheadsystems.dbu.liquibase.LiquibaseHelper;
import com.codeheadsystems.pretender.model.PdbItem;
import com.codeheadsystems.pretender.model.PdbMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;
import java.util.Set;
import javax.inject.Named;
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
   * Instantiates a new Pretender module.
   */
  public PretenderModule() {
    // Default constructor
  }

  /**
   * Jebi jdbi.
   *
   * @param factory         the factory
   * @param liquibaseHelper the liquibase helper
   * @return the jdbi
   */
  @Provides
  @Singleton
  public Jdbi jdbi(final JdbiFactory factory,
                   final LiquibaseHelper liquibaseHelper) {
    final Jdbi jdbi = factory.createJdbi();
    liquibaseHelper.runLiquibase(jdbi, LIQUIBASE_SETUP_XML);
    return jdbi;
    }

    /**
     * Immutable classes set.
     *
     * @return the set
     */
    @Provides
    @Singleton
    @Named(JdbiFactory.IMMUTABLES)
    public Set<Class<?>> immutableClasses() {
      return Set.of(PdbMetadata.class, PdbItem.class);
    }

  /**
   * Object mapper for JSON serialization.
   *
   * @return the object mapper
   */
  @Provides
  @Singleton
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  /**
   * PdbMetadata dao metadata dao.
   *
   * @param jdbi the jdbi
   * @return the metadata dao
   */
  @Provides
  @Singleton
  public PdbMetadataDao metadataDao(final Jdbi jdbi) {
    return jdbi.onDemand(PdbMetadataDao.class);
  }

  /**
   * The interface Binder.
   */
  @Module
  interface Binder {

  }
}
