package com.codeheadsystems.keys.resource;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.api.keys.v1.Keys;
import com.codeheadsystems.keys.converter.KeyConverter;
import com.codeheadsystems.keys.manager.KeyManager;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.server.resource.JerseyResource;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Keys resource.
 */
@Singleton
public class KeysResource implements Keys, JerseyResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeysResource.class);

  private final KeyManager keyManager;
  private final KeyConverter keyConverter;

  /**
   * Instantiates a new Keys resource.
   *
   * @param keyManager   the key manager
   * @param keyConverter the key converter
   */
  @Inject
  public KeysResource(final KeyManager keyManager,
                      final KeyConverter keyConverter) {
    LOGGER.info("KeysResource({},{})", keyManager, keyConverter);
    this.keyManager = keyManager;
    this.keyConverter = keyConverter;
  }

  @Override
  public Key create() {
    LOGGER.trace("create()");
    final RawKey rawKey = keyManager.generateRawKey(256);
    return keyConverter.from(rawKey);
  }
}
