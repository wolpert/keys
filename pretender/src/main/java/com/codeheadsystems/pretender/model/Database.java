package com.codeheadsystems.pretender.model;

import org.immutables.value.Value;

/**
 * The interface Pretender configuration.
 */
@Value.Immutable
public interface Database {

  /**
   * Database url string.
   *
   * @return the string
   */
  String url();

  /**
   * Database username string.
   *
   * @return the string
   */
  String username();

  /**
   * Database password string.
   *
   * @return the string
   */
  String password();

}
