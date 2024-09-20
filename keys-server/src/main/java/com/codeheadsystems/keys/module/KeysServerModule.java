package com.codeheadsystems.keys.module;

import com.codeheadsystems.keys.resource.KeysResource;
import com.codeheadsystems.server.resource.JerseyResource;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import java.security.SecureRandom;
import java.time.Clock;
import javax.inject.Singleton;

/**
 * The type Keys server module.
 */
@Module(includes = KeysServerModule.Binder.class)
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

  @Provides
  @Singleton
  SecureRandom secureRandom() {
    return new SecureRandom();
  }

  @Module
  interface Binder {

    @Binds
    @IntoSet
    JerseyResource keysResource(KeysResource resource);

  }

}
