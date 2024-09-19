package com.codeheadsystems.keys.model;

import java.util.UUID;
import org.immutables.value.Value;

/**
 * RawKey is a value-object for the byte array that represents a decoded key.
 */
@Value.Immutable
public interface RawKey {

  /**
   * Create a RawKey from a byte array.
   *
   * @param uuid the uuid
   * @param key  byte array.
   * @return RawKey. raw key
   */
  static RawKey of(UUID uuid, byte[] key) {
    return ImmutableRawKey.builder().uuid(uuid).key(key).build();
  }

  /**
   * Uuid uuid.
   *
   * @return the uuid
   */
  UUID uuid();

  /**
   * Key.
   *
   * @return byte array.
   */
  byte[] key();

  /**
   * Size of the key in bits.
   *
   * @return integer. int
   */
  @Value.Default
  default int size() {
    return key().length * 8;
  }

}
