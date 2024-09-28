package com.codeheadsystems.pretender.endToEnd;

import com.codeheadsystems.pretender.dagger.PretenderComponent;
import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.model.ImmutableConfiguration;
import com.codeheadsystems.pretender.model.ImmutableDatabase;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseEndToEndTest {

  protected PretenderComponent component;

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
  void setupComponent() {
    component = PretenderComponent.instance(configuration());
  }

}
