package com.codeheadsystems.keys.converter;

import com.codeheadsystems.api.keys.v1.ImmutableKey;
import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.keys.exception.InvalidKeyException;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.keys.utilities.KeyUtilities;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Key converter.
 */
@Singleton
public class KeyConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyConverter.class);

  /**
   * Instantiates a new Key converter.
   */
  @Inject
  public KeyConverter() {
  }

  /**
   * From key.
   *
   * @param rawKey the raw key
   * @return the key
   */
  public Key from(final RawKey rawKey) {
    final String hex = KeyUtilities.encode.apply(rawKey.key());
    return ImmutableKey.builder().key(hex).uuid(rawKey.uuid().toString()).build();
  }

  /**
   * To raw key.
   *
   * @param key the key
   * @return the raw key
   */
  public RawKey to(final Key key) {
    final byte[] bytes = KeyUtilities.decode.apply(key.key())
        .orElseThrow(() -> new InvalidKeyException(key.uuid()));
    final UUID uuid = UUID.fromString(key.uuid());
    return RawKey.of(uuid, bytes);
  }

}
