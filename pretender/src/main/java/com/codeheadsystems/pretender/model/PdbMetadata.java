package com.codeheadsystems.pretender.model;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * The interface PdbMetadata.
 */
@Value.Immutable
public interface PdbMetadata {

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
   * Global Secondary Indexes for this table.
   *
   * @return the list of GSIs
   */
  @Value.Default
  default List<PdbGlobalSecondaryIndex> globalSecondaryIndexes() {
    return List.of();
  }

  /**
   * The attribute name used for Time-To-Live (TTL).
   * When present, items will be automatically deleted when the TTL timestamp expires.
   *
   * @return the TTL attribute name
   */
  Optional<String> ttlAttributeName();

  /**
   * Whether TTL is enabled for this table.
   *
   * @return true if TTL is enabled
   */
  @Value.Default
  default boolean ttlEnabled() {
    return false;
  }

  /**
   * Create date instant.
   *
   * @return the instant
   */
  Instant createDate();

}
