package com.codeheadsystems.keys.model;

import com.codeheadsystems.keys.utilities.KeyUtilities;
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
    return ImmutableRawKey.builder().uuid(uuid.toString()).key(KeyUtilities.encode.apply(key)).size(key.length).build();
  }

  /**
   * Of raw key.
   *
   * @param uuid the uuid
   * @param key  the key
   * @param size the size
   * @return the raw key
   */
  static RawKey of(String uuid, String key, int size) {
    return ImmutableRawKey.builder().uuid(uuid).key(key).size(size).build();
  }

  /**
   * Uuid uuid.
   *
   * @return the uuid
   */
  String uuid();

  /**
   * Key.
   *
   * @return byte array.
   */
  String key();

  /**
   * Size of the key in bits.
   *
   * @return integer. int
   */
  int size();

}
