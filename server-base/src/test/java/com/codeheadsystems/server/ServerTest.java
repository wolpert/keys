package com.codeheadsystems.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.health.HealthCheck;
import com.codeheadsystems.server.component.DropWizardComponent;
import com.codeheadsystems.server.module.DropWizardModule;
import com.codeheadsystems.server.resource.JerseyResource;
import dagger.Binds;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.dropwizard.core.Application;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith(DropwizardExtensionsSupport.class)
class ServerTest {

  private static final String HELLO_WORLD = "Hello World";
  private static final DropwizardAppExtension<ServerTestConfiguration> EXT = new DropwizardAppExtension<>(
      ServerTestServer.class,
      ResourceHelpers.resourceFilePath("server-test.yml")
  );

  @Test
  void testServerExists() {
    final Application<ServerTestConfiguration> application = EXT.getApplication();
    assertThat(application).isNotNull();
  }

  @Test
  void testResourceCall() {
    final String response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/hello")
        .request()
        .get(String.class);
    assertThat(response).isEqualTo(HELLO_WORLD);
  }

  /**
   * Creates the pieces needed for the control plane to run. Required.
   */
  @Component(modules = {
      DropWizardModule.class,
      ServerTestBinderModule.class,
      ServerTestConfigurationModule.class
  })
  @Singleton
  public interface ServerTestDropWizardComponent extends DropWizardComponent {
  }

  /**
   * A binder interface if you don't want to have explicit objects. Optional.
   */
  @Module
  public interface ServerTestBinderModule {
    @Binds
    @IntoSet
    JerseyResource helloWorldResource(final HelloWorldResource resource);
  }

  /**
   * The server itself, What will have the main class to run when not a test.
   */
  public static class ServerTestServer extends Server<ServerTestConfiguration> {
    @Override
    protected DropWizardComponent dropWizardComponent(final DropWizardModule module) {
      return DaggerServerTest_ServerTestDropWizardComponent.builder()
          .dropWizardModule(module)
          .build();
    }
  }

  /**
   * Define some objects to be injected via code.
   */
  @Module
  public static class ServerTestConfigurationModule {

    @Provides
    @Singleton
    Clock clock() {
      return Clock.systemUTC();
    }

    @Provides
    @Singleton
    @IntoSet
    HealthCheck fakeHealthCheck() {
      return new HealthCheck() {
        @Override
        protected Result check() {
          return Result.healthy();
        }
      };
    }

  }

  /**
   * A sample resource to expose with dropwizard.
   */
  @Singleton
  @Path("/")
  public static class HelloWorldResource implements JerseyResource {

    @Inject
    public HelloWorldResource() {
    }

    @GET
    @Path("hello")
    public String hello() {
      LoggerFactory.getLogger(HelloWorldResource.class).info("hello()");
      return HELLO_WORLD;
    }
  }

  /**
   * The configuration of the dropwizard server. Required.
   */
  public static class ServerTestConfiguration extends ServerConfiguration {

  }


}