package com.codeheadsystems.keys.integ;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.keys.KeysServer;
import com.codeheadsystems.keys.KeysServerConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
public class KeyCreationTest {

  private static final DropwizardAppExtension<KeysServerConfiguration> EXT = new DropwizardAppExtension<>(
      KeysServer.class,
      ResourceHelpers.resourceFilePath("server-test.yml")
  );

  @Test
  void createAndGet() throws InterruptedException {
    final Response response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/v1/keys/")
        .request()
        .post(Entity.text("blah")); // we really do not need an entity.
    assertThat(response)
        .isNotNull()
        .hasFieldOrPropertyWithValue("status", 201);
    final Key key = response.readEntity(Key.class);
    assertThat(key)
        .hasFieldOrProperty("uuid");

    // finally, test location
    final String location = response.getHeaderString("Location");
    final Key key2 = EXT.client().target(location)
        .request()
        .get(Key.class);
    assertThat(key2).isEqualTo(key);
  }

  @Test
  void get_notFound() {
    final UUID uuid = UUID.randomUUID();
    final Response response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/v1/keys/" + uuid)
        .request()
        .get();
    assertThat(response)
        .isNotNull()
        .hasFieldOrPropertyWithValue("status", 404);
  }

  @Test
  void badRequest() {
    final Response response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/v1/bad")
        .request()
        .get();
    assertThat(response)
        .isNotNull()
        .hasFieldOrPropertyWithValue("status", 404);
  }
}
