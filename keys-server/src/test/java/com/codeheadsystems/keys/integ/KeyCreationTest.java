package com.codeheadsystems.keys.integ;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.keys.KeysServer;
import com.codeheadsystems.keys.KeysServerConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
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
  void create() throws InterruptedException {
    final Key response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/v1/keys/")
        .request()
        .put(Entity.entity("test", MediaType.APPLICATION_JSON_TYPE), Key.class);
    assertThat(response)
        .isNotNull()
        .hasFieldOrProperty("key");
  }

  @Test
  void get() {
    final UUID uuid = UUID.randomUUID();
    final Key response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/v1/keys/" + uuid)
        .request()
        .get(Key.class);
    assertThat(response)
        .isNotNull()
        .hasFieldOrProperty("key");
  }

  @Test
  void bad() {
    final Response response = EXT.client().target("http://localhost:" + EXT.getLocalPort() + "/v1/bad")
        .request()
        .get();
    assertThat(response)
        .isNotNull()
        .hasFieldOrPropertyWithValue("status", 404);
  }
}
