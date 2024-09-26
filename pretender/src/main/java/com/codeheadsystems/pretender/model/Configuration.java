package com.codeheadsystems.pretender.model;

import org.immutables.value.Value;

/**
 * The interface Pretender configuration.
 */
@Value.Immutable
public interface Configuration {

  /**
   * Database database.
   *
   * @return the database
   */
  Database database();

}
