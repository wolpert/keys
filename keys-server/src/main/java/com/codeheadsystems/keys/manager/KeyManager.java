package com.codeheadsystems.keys.manager;

import com.codeheadsystems.keys.dao.RawKeyDao;
import com.codeheadsystems.keys.model.ImmutableRawKey;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.metrics.declarative.Metrics;
import com.codeheadsystems.metrics.declarative.Tag;
import com.codeheadsystems.server.exception.NotFoundException;
import java.security.SecureRandom;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Key manager.
 */
@Singleton
public class KeyManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyManager.class);

  private final SecureRandom secureRandom;
  private final RawKeyDao rawKeyDao;

  /**
   * Instantiates a new Key manager.
   *
   * @param secureRandom the secure random
   * @param rawKeyDao    the raw key dao
   */
  @Inject
  public KeyManager(final SecureRandom secureRandom,
                    final RawKeyDao rawKeyDao) {
    this.rawKeyDao = rawKeyDao;
    LOGGER.info("KeyManager({})", secureRandom);
    this.secureRandom = secureRandom;
  }

  /**
   * Generate raw key raw key.
   *
   * @param size the size
   * @return the raw key
   */
  @Metrics
  public RawKey generateRawKey(@Tag("size") int size) {
    LOGGER.trace("generateRawKey({}))", size);
    if (size % 8 != 0) {
      throw new IllegalArgumentException("Size must be a multiple of 8");
    }
    byte[] key = new byte[size / 8];
    secureRandom.nextBytes(key);
    final RawKey rawKey = RawKey.of(UUID.randomUUID(), key);
    final boolean inserted;
    try {
      inserted = rawKeyDao.insert(rawKey);
    } catch (RuntimeException e) {
      throw new IllegalStateException("Failure to insert key into database", e);
    }
    if(inserted) {
      return rawKey;
    } else {
      throw new IllegalStateException("Unable to insert key into database");
    }
  }

  @Metrics
  public RawKey getRawKey(final String uuid) {
    LOGGER.trace("getRawKey({})", uuid);
    return rawKeyDao.getKey(uuid)
        .orElseThrow(() -> new NotFoundException("Key not found"));
  }

}
