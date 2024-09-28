package com.codeheadsystems.pretender.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import com.codeheadsystems.pretender.model.ImmutablePdbTable;
import com.codeheadsystems.pretender.model.PdbTable;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

@ExtendWith(MockitoExtension.class)
class PdbTableConverterTest {

  private static final Instant NOW = Instant.now();

  @Mock private Clock clock;

  @InjectMocks private PdbTableConverter converter;

  @Test
  void testConvert() {
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName("p_sortKey").keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName("tableName")
        .keySchema(keySchemaElements)
        .build();
    when(clock.instant()).thenReturn(NOW);

    assertThat(converter.convert(createTableRequest))
        .hasFieldOrPropertyWithValue("name", "tableName")
        .hasFieldOrPropertyWithValue("hashKey", "p_hashKey")
        .hasFieldOrPropertyWithValue("sortKey", "p_sortKey")
        .hasFieldOrPropertyWithValue("createDate", NOW);
  }

  @Test
  void testConvert_noSort() {
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName("tableName")
        .keySchema(keySchemaElements)
        .build();
    when(clock.instant()).thenReturn(NOW);

    assertThat(converter.convert(createTableRequest))
        .hasFieldOrPropertyWithValue("name", "tableName")
        .hasFieldOrPropertyWithValue("hashKey", "p_hashKey")
        .hasFieldOrPropertyWithValue("sortKey", null)
        .hasFieldOrPropertyWithValue("createDate", NOW);
  }

  @Test
  void testConvert_noHash() {
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName("p_sortKey").keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName("tableName")
        .keySchema(keySchemaElements)
        .build();

    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> converter.convert(createTableRequest));
  }

  @Test
  public void testFromPdbTable() {
    final PdbTable pdbTable = ImmutablePdbTable.builder()
        .name("tableName")
        .hashKey("p_hashKey")
        .sortKey("p_sortKey")
        .createDate(NOW)
        .build();
    final TableDescription tableDescription = converter.fromPdbTable(pdbTable);
    assertThat(tableDescription)
        .hasFieldOrPropertyWithValue("tableName", "tableName")
        .hasFieldOrPropertyWithValue("keySchema", List.of(
            KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("p_sortKey").keyType(KeyType.RANGE).build()
        ));
  }

  @Test
  public void testFromPdbTable_noSort() {
    final PdbTable pdbTable = ImmutablePdbTable.builder()
        .name("tableName")
        .hashKey("p_hashKey")
        .createDate(NOW)
        .build();
    final TableDescription tableDescription = converter.fromPdbTable(pdbTable);
    assertThat(tableDescription)
        .hasFieldOrPropertyWithValue("tableName", "tableName")
        .hasFieldOrPropertyWithValue("keySchema", List.of(
            KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build()
        ));
  }

}