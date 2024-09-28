package com.codeheadsystems.pretender;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.pretender.converter.PdbTableConverter;
import com.codeheadsystems.pretender.manager.PdbTableManager;
import com.codeheadsystems.pretender.manager.PretenderDatabaseManager;
import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.model.ImmutableConfiguration;
import com.codeheadsystems.pretender.model.ImmutableDatabase;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DynamoDbPretenderClientTest {

  @Mock private PretenderDatabaseManager pretenderDatabaseManager;
  @Mock private PdbTableManager pdbTableManager;
  @Mock private PdbTableConverter pdbTableConverter;

  private DynamoDbPretenderClient client;

  @BeforeEach
  void setup() {
    client = new DynamoDbPretenderClient(pretenderDatabaseManager, pdbTableManager, pdbTableConverter);
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  @Test
  void serviceName() {
    assertThat(client.serviceName()).isEqualTo("dynamodb");
  }

}