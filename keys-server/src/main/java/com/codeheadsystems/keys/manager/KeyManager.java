package com.codeheadsystems.keys.manager;

import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.metrics.Metrics;
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
  private final Metrics metrics;

  /**
   * Instantiates a new Key manager.
   *
   * @param secureRandom the secure random
   */
  @Inject
  public KeyManager(final SecureRandom secureRandom,
                    final Metrics metrics) {
    LOGGER.info("KeyManager({}, {})", secureRandom, metrics);
    this.secureRandom = secureRandom;
    this.metrics = metrics;
  }

  /**
   * Generate raw key raw key.
   *
   * @param size the size
   * @return the raw key
   */
  public RawKey generateRawKey(int size) {
    LOGGER.trace("generateRawKey({}))", size);
    if (size % 8 != 0) {
      throw new IllegalArgumentException("Size must be a multiple of 8");
    }
    byte[] key = new byte[size / 8];
    secureRandom.nextBytes(key);
    return RawKey.of(UUID.randomUUID(), key);
  }

}
