package com.codeheadsystems.pretender.model;

import java.time.Instant;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * The interface Metadata.
 */
@Value.Immutable
public interface Metadata {

  /**
   * Hash key string.
   *
   * @return the string
   */
  String hashKey();

  /**
   * Sort key optional.
   *
   * @return the optional
   */
  Optional<String> sortKey();

  /**
   * Create date instant.
   *
   * @return the instant
   */
  Instant createDate();

}
