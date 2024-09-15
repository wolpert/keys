package com.codeheadsystems.keys.module;

import dagger.Module;
import dagger.Provides;
import java.time.Clock;
import javax.inject.Singleton;

/**
 * The type Keys server module.
 */
@Module
public class KeysServerModule {

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

}
