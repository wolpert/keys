package com.codeheadsystems.pretender.model;

import java.time.Instant;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * The interface PdbTable.
 */
@Value.Immutable
public interface PdbTable {

  /**
   * Name string.
   *
   * @return the string
   */
  String name();

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
