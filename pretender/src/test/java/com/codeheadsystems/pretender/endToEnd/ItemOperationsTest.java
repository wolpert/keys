package com.codeheadsystems.pretender.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codeheadsystems.pretender.DynamoDbPretenderClient;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class ItemOperationsTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "ItemOpsTable";
  private static final String HASH_KEY = "userId";
  private static final String SORT_KEY = "timestamp";

  private DynamoDbPretenderClient client;

  @BeforeEach
  void setup() {
    client = component.dynamoDbPretenderClient();

    // Create table with hash and sort keys
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(keySchemaElements)
        .build();
    client.createTable(createTableRequest);
  }

  @AfterEach
  void teardown() {
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
    } catch (ResourceNotFoundException e) {
      // Table may already be deleted in some tests
    }
  }

  @Test
  void putItem_and_getItem_roundTrip() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "age", AttributeValue.builder().n("30").build(),
        "email", AttributeValue.builder().s("john@example.com").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    final PutItemResponse putResponse = client.putItem(putRequest);
    assertThat(putResponse).isNotNull();

    // Get the item back
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.item()).isNotNull();
    assertThat(getResponse.item()).hasSize(5);
    assertThat(getResponse.item().get("name").s()).isEqualTo("John Doe");
    assertThat(getResponse.item().get("age").n()).isEqualTo("30");
    assertThat(getResponse.item().get("email").s()).isEqualTo("john@example.com");
  }

  @Test
  void getItem_withProjection() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "age", AttributeValue.builder().n("30").build(),
        "email", AttributeValue.builder().s("john@example.com").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get with projection
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .projectionExpression("name, age")
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.item()).hasSize(2);
    assertThat(getResponse.item()).containsKey("name");
    assertThat(getResponse.item()).containsKey("age");
    assertThat(getResponse.item()).doesNotContainKey("email");
  }

  @Test
  void getItem_notFound() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("nonexistent").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void updateItem_setAttributes() {
    // Put initial item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "count", AttributeValue.builder().n("5").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Update with SET expression
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("SET #n = :newName, count = count + :inc")
        .expressionAttributeNames(Map.of("#n", "name"))
        .expressionAttributeValues(Map.of(
            ":newName", AttributeValue.builder().s("Jane Doe").build(),
            ":inc", AttributeValue.builder().n("3").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse updateResponse = client.updateItem(updateRequest);
    assertThat(updateResponse.attributes().get("name").s()).isEqualTo("Jane Doe");
    assertThat(updateResponse.attributes().get("count").n()).isEqualTo("8");
  }

  @Test
  void updateItem_createNewItem() {
    // Update on non-existent item should create it
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-456").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build()
    );

    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("SET #n = :name, count = :count")
        .expressionAttributeNames(Map.of("#n", "name"))
        .expressionAttributeValues(Map.of(
            ":name", AttributeValue.builder().s("New User").build(),
            ":count", AttributeValue.builder().n("1").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse updateResponse = client.updateItem(updateRequest);
    assertThat(updateResponse.attributes()).containsKey("name");
    assertThat(updateResponse.attributes()).containsKey("count");

    // Verify it was created
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build());
    assertThat(getResponse.item().get("name").s()).isEqualTo("New User");
  }

  @Test
  void updateItem_removeAttributes() {
    // Put initial item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "email", AttributeValue.builder().s("john@example.com").build(),
        "age", AttributeValue.builder().n("30").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Remove email
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("REMOVE email")
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse updateResponse = client.updateItem(updateRequest);
    assertThat(updateResponse.attributes()).doesNotContainKey("email");
    assertThat(updateResponse.attributes()).containsKey("name");
    assertThat(updateResponse.attributes()).containsKey("age");
  }

  @Test
  void deleteItem_withReturnValues() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-789").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Deleted User").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete with return values
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-789").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build()
    );

    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .returnValues(ReturnValue.ALL_OLD)
        .build();

    final DeleteItemResponse deleteResponse = client.deleteItem(deleteRequest);
    assertThat(deleteResponse.attributes()).isNotNull();
    assertThat(deleteResponse.attributes().get("name").s()).isEqualTo("Deleted User");

    // Verify it's gone
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build());
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void query_withHashKey() {
    // Put multiple items with same hash key
    for (int i = 1; i <= 3; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-123").build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-0" + i + "T00:00:00Z").build(),
          "data", AttributeValue.builder().s("Item " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Query by hash key
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-123").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).hasSize(3);
    assertThat(queryResponse.count()).isEqualTo(3);
  }

  @Test
  void query_withHashAndSortKeyCondition() {
    // Put multiple items
    for (int i = 1; i <= 5; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-456").build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-0" + i + "T00:00:00Z").build(),
          "data", AttributeValue.builder().s("Item " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Query with sort key condition (without expression attribute names for now)
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid AND timestamp < :ts")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-456").build(),
            ":ts", AttributeValue.builder().s("2024-01-04T00:00:00Z").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).hasSize(3);  // Items 1, 2, 3
  }

  @Test
  void query_withLimit() {
    // Put multiple items
    for (int i = 1; i <= 10; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-789").build(),
          SORT_KEY, AttributeValue.builder().s(String.format("2024-01-%02dT00:00:00Z", i)).build(),
          "data", AttributeValue.builder().s("Item " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Query with limit
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-789").build()
        ))
        .limit(5)
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).hasSize(5);
    assertThat(queryResponse.count()).isEqualTo(5);
    assertThat(queryResponse.hasLastEvaluatedKey()).isTrue();
  }

  @Test
  void scan_allItems() {
    // Put items from different users
    for (int i = 1; i <= 3; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "data", AttributeValue.builder().s("User " + i + " data").build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Scan all
    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .build();

    final ScanResponse scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSizeGreaterThanOrEqualTo(3);
    assertThat(scanResponse.count()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void scan_withLimit() {
    // Put many items
    for (int i = 1; i <= 10; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "data", AttributeValue.builder().s("Data " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Scan with limit
    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(5)
        .build();

    final ScanResponse scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(5);
    assertThat(scanResponse.count()).isEqualTo(5);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();
  }

  @Test
  void operations_onNonExistentTable() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    assertThatThrownBy(() ->
        client.getItem(GetItemRequest.builder()
            .tableName("NonExistentTable")
            .key(key)
            .build())
    ).isInstanceOf(ResourceNotFoundException.class);
  }

  @Test
  void complexWorkflow_putUpdateQueryDelete() {
    // 1. Put multiple items
    for (int i = 1; i <= 3; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("workflow-user").build(),
          SORT_KEY, AttributeValue.builder().s("event-" + i).build(),
          "status", AttributeValue.builder().s("pending").build(),
          "count", AttributeValue.builder().n("0").build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // 2. Update one item
    client.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("workflow-user").build(),
            SORT_KEY, AttributeValue.builder().s("event-2").build()
        ))
        .updateExpression("SET #s = :status, count = count + :inc")
        .expressionAttributeNames(Map.of("#s", "status"))
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("complete").build(),
            ":inc", AttributeValue.builder().n("5").build()
        ))
        .build());

    // 3. Query all items for this user
    final QueryResponse queryResponse = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("workflow-user").build()
        ))
        .build());

    assertThat(queryResponse.items()).hasSize(3);

    // Verify updated item
    final var event2 = queryResponse.items().stream()
        .filter(item -> item.get(SORT_KEY).s().equals("event-2"))
        .findFirst()
        .orElseThrow();
    assertThat(event2.get("status").s()).isEqualTo("complete");
    assertThat(event2.get("count").n()).isEqualTo("5");

    // 4. Delete one item
    client.deleteItem(DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("workflow-user").build(),
            SORT_KEY, AttributeValue.builder().s("event-1").build()
        ))
        .build());

    // 5. Verify deletion
    final QueryResponse afterDeleteQuery = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("workflow-user").build()
        ))
        .build());

    assertThat(afterDeleteQuery.items()).hasSize(2);
  }
}
