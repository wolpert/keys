package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.DynamoDbPretenderClient;
import com.codeheadsystems.pretender.model.Configuration;
import dagger.Component;
import javax.inject.Singleton;

/**
 * The interface Pretender component.
 */
@Singleton
@Component(modules = {PretenderModule.class, ConfigurationModule.class, CommonModule.class})
public interface PretenderComponent {

  /**
   * Instance pretender component.
   *
   * @param configuration the configuration
   * @return the pretender component
   */
  static PretenderComponent instance(final Configuration configuration) {
    return DaggerPretenderComponent.builder().configurationModule(new ConfigurationModule(configuration)).build();
  }

  /**
   * Pretender database manager pretender database manager.
   *
   * @return the pretender database manager
   */
  DynamoDbPretenderClient dynamoDbPretenderClient();
}
