package com.codeheadsystems.pretender.dagger;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.model.ImmutableConfiguration;
import com.codeheadsystems.pretender.model.ImmutableDatabase;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PretenderComponentTest {

  private PretenderComponent component;

  private Configuration configuration() {
    return ImmutableConfiguration.builder()
        .database(
            ImmutableDatabase.builder()
                .url("jdbc:hsqldb:mem:PretenderComponentTest" + UUID.randomUUID())
                .username("SA")
                .password("")
                .build())
        .build();
  }

  @BeforeEach
  void setup() {
    component = PretenderComponent.instance(configuration());
  }

  @Test
  void testCreateManager() {
    assertThat(component.pretenderDatabaseManager()).isNotNull();
  }

}