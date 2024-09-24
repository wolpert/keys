package com.codeheadsystems.keys.resource;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.api.keys.v1.Keys;
import com.codeheadsystems.keys.converter.KeyConverter;
import com.codeheadsystems.keys.manager.KeyManager;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.server.resource.JerseyResource;
import jakarta.ws.rs.core.Response;
import java.net.URI;
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
  public Response create() {
    LOGGER.trace("create()");
    final RawKey rawKey = keyManager.generateRawKey(256);
    final Key key = keyConverter.from(rawKey);
    return Response.created(URI.create("/v1/keys/" + key.uuid()))
        .entity(key)
        .build();
  }

  @Override
  public Key read(final String uuid) {
    LOGGER.trace("get({})", uuid);
    final RawKey rawKey = keyManager.rawKey(uuid);
    return keyConverter.from(rawKey);
  }

  @Override
  public Response delete(final String uuid) {
    return Response.accepted()
        .build();
  }
}
