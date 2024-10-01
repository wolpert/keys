package com.codeheadsystems.pretender.dagger;

import dagger.Module;
import dagger.Provides;
import java.time.Clock;
import javax.inject.Singleton;

/**
 * The type Common module.
 */
@Module
public class CommonModule {

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
