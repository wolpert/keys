package com.codeheadsystems.server.initializer;

import io.dropwizard.core.setup.Environment;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Server initializer.
 */
@Singleton
public class ServerInitializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerInitializer.class);

  private final HealthCheckInitializer healthCheckInitializer;
  private final ManagedObjectInitializer managedObjectInitializer;
  private final JerseyResourceInitializer jerseyResourceInitializer;
  private final Environment environment;
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  /**
   * Instantiates a new Server initializer.
   *
   * @param healthCheckInitializer    the health check initializer
   * @param managedObjectInitializer  the managed object initializer
   * @param jerseyResourceInitializer the jersey resource initializer
   * @param environment               the environment
   */
  @Inject
  public ServerInitializer(final HealthCheckInitializer healthCheckInitializer,
                           final ManagedObjectInitializer managedObjectInitializer,
                           final JerseyResourceInitializer jerseyResourceInitializer,
                           final Environment environment) {
    LOGGER.trace("ServerInitializer({}, {}, {}, {})",
        healthCheckInitializer, managedObjectInitializer, jerseyResourceInitializer, environment);
    this.healthCheckInitializer = healthCheckInitializer;
    this.managedObjectInitializer = managedObjectInitializer;
    this.jerseyResourceInitializer = jerseyResourceInitializer;
    this.environment = environment;
  }

  /**
   * Initialize.
   */
  public void initialize() {
    LOGGER.info("\n---\n--- Initializing Server ---\n---");
    synchronized (initialized) {
      if (initialized.get()) {
        LOGGER.warn("Server already initialized, skipping.");
        return;
      }
      try {
        jerseyResourceInitializer.initialize(environment);
        managedObjectInitializer.initialize(environment);
        healthCheckInitializer.initialize(environment);
        LOGGER.info("\n---\n--- Server Initialized ---\n---");
      } catch (RuntimeException e) {
        LOGGER.error("Failed to initialize server. State is unknown.", e);
        throw e;
      } finally {
        initialized.set(true);
      }
    }
  }

}
