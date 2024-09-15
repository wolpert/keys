package com.codeheadsystems.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.server.component.DropWizardComponent;
import com.codeheadsystems.server.module.DropWizardModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.dropwizard.core.Application;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import java.time.Clock;
import javax.inject.Singleton;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServerTest {

  private DropwizardTestSupport<ServerTestConfiguration> dropwizardTestSupport;

  @BeforeEach
  void setup() throws Exception {
    dropwizardTestSupport = new DropwizardTestSupport<>(
        ServerTestServer.class,
        ResourceHelpers.resourceFilePath("server-test.yml"));
    dropwizardTestSupport.before();
  }

  @AfterEach
  void tearDown() {
    dropwizardTestSupport.after();
  }

  @Test
  void testServerExists() {
    final Application<ServerTestConfiguration> application = dropwizardTestSupport.getApplication();
    assertThat(application).isNotNull();
  }

  /**
   * Creates the pieces needed for the control plane to run.
   */
  @Component(modules = {
      DropWizardModule.class,
      ServerTestConfigurationModule.class
  })
  @Singleton
  public interface ServerTestDropWizardComponent extends DropWizardComponent {
  }

  public static class ServerTestServer extends Server<ServerTestConfiguration> {
    @Override
    protected DropWizardComponent dropWizardComponent(final DropWizardModule module) {
      return DaggerServerTest_ServerTestDropWizardComponent.builder()
          .dropWizardModule(module)
          .build();
    }
  }

  @Module
  public static class ServerTestConfigurationModule {

    @Provides
    @Singleton
    Clock clock() {
      return Clock.systemUTC();
    }

  }


}