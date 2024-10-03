package com.codeheadsystems.keys.resource;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.api.keys.v1.Keys;
import com.codeheadsystems.keys.converter.KeyConverter;
import com.codeheadsystems.keys.manager.KeyManager;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.server.resource.JerseyResource;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
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
    final URI uri = UriBuilder.fromResource(Keys.class).path(key.uuid()).build();
    return Response.created(uri).entity(key).build();
  }

  @Override
  public Key read(final String uuid) {
    LOGGER.trace("get({})", uuid);
    final RawKey rawKey = keyManager.getRawKey(uuid);
    return keyConverter.from(rawKey);
  }

  @Override
  public Response delete(final String uuid) {
    return Response.noContent().build();
  }
}
