package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.dbu.factory.JdbiFactory;
import com.codeheadsystems.dbu.liquibase.LiquibaseHelper;
import com.codeheadsystems.pretender.dao.PdbMetadataDao;
import com.codeheadsystems.pretender.model.PdbGlobalSecondaryIndex;
import com.codeheadsystems.pretender.model.PdbItem;
import com.codeheadsystems.pretender.model.PdbMetadata;
import com.codeheadsystems.pretender.model.PdbStreamRecord;
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
   * @param objectMapper    the object mapper
   * @return the jdbi
   */
  @Provides
  @Singleton
  public Jdbi jdbi(final JdbiFactory factory,
                   final LiquibaseHelper liquibaseHelper,
                   final ObjectMapper objectMapper) {
    final Jdbi jdbi = factory.createJdbi();
    liquibaseHelper.runLiquibase(jdbi, LIQUIBASE_SETUP_XML);

    // Register custom mappers for GSI list serialization
    jdbi.registerArgument(new com.codeheadsystems.pretender.dao.GsiListArgumentFactory(objectMapper));
    jdbi.registerColumnMapper(new com.codeheadsystems.pretender.dao.GsiListColumnMapper(objectMapper));

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
    return Set.of(PdbMetadata.class, PdbItem.class, PdbGlobalSecondaryIndex.class, PdbStreamRecord.class);
  }

  /**
   * Object mapper for JSON serialization.
   *
   * @return the object mapper
   */
  @Provides
  @Singleton
  public ObjectMapper objectMapper() {
    final ObjectMapper objectMapper = new ObjectMapper();
    // Register all available Jackson modules including Java 8 date/time support
    objectMapper.findAndRegisterModules();
    // Configure to handle unknown properties gracefully
    objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // Enable detection of fields without getters (for Immutables)
    objectMapper.setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.FIELD,
        com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY);
    return objectMapper;
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
