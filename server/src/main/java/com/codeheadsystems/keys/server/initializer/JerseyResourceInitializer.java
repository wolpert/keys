package com.codeheadsystems.keys.server.initializer;

import static org.slf4j.LoggerFactory.getLogger;

import com.codeheadsystems.keys.server.resource.JerseyResource;
import io.dropwizard.core.setup.Environment;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
/**
 * Initializes all Jersey Resources.
 */
@Singleton
public class JerseyResourceInitializer implements Initializer {

  private static final Logger LOGGER = getLogger(JerseyResourceInitializer.class);

  private final Set<JerseyResource> jerseyResources;

  /**
   * Constructor.
   *
   * @param jerseyResources to initialize.
   */
  @Inject
  public JerseyResourceInitializer(final Set<JerseyResource> jerseyResources) {
    LOGGER.trace("JerseyResourceInitializer({})", jerseyResources);
    this.jerseyResources = jerseyResources;
  }


  @Override
  public void initialize(final Environment environment) {
    LOGGER.info("\n---\n--- Registering Resources ---\n---");
    for (Object resource : jerseyResources) {
      LOGGER.info("Registering resource: {}", resource.getClass().getSimpleName());
      environment.jersey().register(resource);
    }
  }
}
