package com.codeheadsystems.keys.module;

import com.codeheadsystems.dbu.factory.JdbiFactory;
import com.codeheadsystems.dbu.liquibase.LiquibaseHelper;
import com.codeheadsystems.dbu.model.Database;
import com.codeheadsystems.dbu.model.ImmutableDatabase;
import com.codeheadsystems.keys.dao.RawKeyDao;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.keys.resource.InvalidKeyExceptionMapper;
import com.codeheadsystems.keys.resource.KeysResource;
import com.codeheadsystems.server.resource.JerseyResource;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.Set;
import java.util.UUID;
import javax.inject.Named;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;

/**
 * The type Keys server module.
 */
@Module(includes = KeysServerModule.Binder.class)
public class KeysServerModule {

  /**
   * The constant LIQUIBASE_SETUP_XML.
   */
  public static final String LIQUIBASE_SETUP_XML = "liquibase/liquibase-setup.xml";

  /**
   * Clock clock.
   *
   * @return the clock
   */
  @Provides
  @Singleton
  Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * Secure random secure random.
   *
   * @return the secure random
   */
  @Provides
  @Singleton
  SecureRandom secureRandom() {
    return new SecureRandom();
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
    return Set.of(RawKey.class);
  }

  /**
   * Database database.
   *
   * @return the database
   */
  @Provides
  @Singleton
  public Database database() {
    return ImmutableDatabase.builder()
        .url("jdbc:hsqldb:mem:PretenderComponentTest" + UUID.randomUUID())
        .username("SA")
        .password("")
        .build();
  }

  /**
   * PdbMetadata dao metadata dao.
   *
   * @param jdbi the jdbi
   * @return the metadata dao
   */
  @Provides
  @Singleton
  public RawKeyDao metadataDao(final Jdbi jdbi) {
    return jdbi.onDemand(RawKeyDao.class);
  }

  /**
   * The interface Binder.
   */
  @Module
  interface Binder {

    /**
     * Keys resource jersey resource.
     *
     * @param resource the resource
     * @return the jersey resource
     */
    @Binds
    @IntoSet
    JerseyResource keysResource(KeysResource resource);

    /**
     * invalid key mapper.
     *
     * @param resource resource.
     * @return JerseyResource. jersey resource
     */
    @Binds
    @IntoSet
    JerseyResource invalidKeyExceptionMapper(InvalidKeyExceptionMapper resource);

  }

}
