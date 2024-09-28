package com.codeheadsystems.pretender.dagger;

import dagger.Module;
import dagger.Provides;
import java.time.Clock;
import javax.inject.Singleton;

@Module
public class CommonModule {

  @Provides
  @Singleton
  Clock clock() {
    return Clock.systemUTC();
  }

}
