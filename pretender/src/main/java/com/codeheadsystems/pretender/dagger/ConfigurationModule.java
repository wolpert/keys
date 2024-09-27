package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.model.Configuration;
import dagger.Module;
import dagger.Provides;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * The type Configuration module.
 */
@Module
public class ConfigurationModule {

  /**
   * The constant RUN_LIQUIBASE.
   */
  public static final String RUN_LIQUIBASE = "runLiquibase_config";


  private final Configuration configuration;
  private final boolean runLiquibase;


  /**
   * Instantiates a new Configuration module.
   *
   * @param configuration the configuration
   */
  public ConfigurationModule(final Configuration configuration) {
    this(configuration, true);
  }

  /**
   * Instantiates a new Configuration module.
   *
   * @param configuration the configuration
   * @param runLiquibase  the run liquibase
   */
  public ConfigurationModule(final Configuration configuration,
                             final boolean runLiquibase) {
    this.configuration = configuration;
    this.runLiquibase = runLiquibase;
  }

  /**
   * Configuration configuration.
   *
   * @return the configuration
   */
  @Provides
  @Singleton
  public Configuration configuration() {
    return configuration;
  }

  /**
   * Run liquibase boolean.
   *
   * @return the boolean
   */
  @Provides
  @Singleton
  @Named(RUN_LIQUIBASE)
  public boolean runLiquibase() {
    return runLiquibase;
  }
}
