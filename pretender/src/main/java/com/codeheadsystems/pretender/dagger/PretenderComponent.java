package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.manager.PretenderDatabaseManager;
import dagger.Component;
import javax.inject.Singleton;

/**
 * The interface Pretender component.
 */
@Singleton
@Component(modules = {PretenderModule.class, ConfigurationModule.class})
public interface PretenderComponent {

  /**
   * Pretender database manager pretender database manager.
   *
   * @return the pretender database manager
   */
  PretenderDatabaseManager pretenderDatabaseManager();

}
