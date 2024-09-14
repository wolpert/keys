package com.codeheadsystems.server.initializer;

import io.dropwizard.core.setup.Environment;

/**
 * Interface for all initializers.
 */
@FunctionalInterface
public interface Initializer {

  /**
   * Does the initialization.
   *
   * @param environment to initialize in.
   */
  void initialize(final Environment environment);

}
