package com.codeheadsystems.api.keys.v1;

import org.immutables.value.Value;

/**
 * The interface Key.
 */
@Value.Immutable
public interface Key {

  /**
   * Key string.
   *
   * @return the string
   */
  String key();

}
