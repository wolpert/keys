package com.codeheadsystems.pretender.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

/**
 * Internal model for shard iterator information.
 * Used for encoding/decoding shard iterator strings.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableShardIterator.class)
@JsonDeserialize(as = ImmutableShardIterator.class)
public interface ShardIterator {

  /**
   * The table name.
   *
   * @return the table name
   */
  String tableName();

  /**
   * The shard ID (always "shard-00000" for pretender).
   *
   * @return the shard id
   */
  String shardId();

  /**
   * The sequence number position for this iterator.
   *
   * @return the sequence number
   */
  long sequenceNumber();

  /**
   * The shard iterator type.
   *
   * @return the type
   */
  ShardIteratorType type();
}
