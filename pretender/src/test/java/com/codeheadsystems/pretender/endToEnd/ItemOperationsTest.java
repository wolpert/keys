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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
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

  @Test
  void putItem_withConditionExpression_attributeNotExists_success() {
    // First put should succeed (item doesn't exist yet)
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-1").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Conditional User").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .conditionExpression("attribute_not_exists(userId)")
        .build();

    final PutItemResponse putResponse = client.putItem(putRequest);
    assertThat(putResponse).isNotNull();

    // Verify item was created
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("name").s()).isEqualTo("Conditional User");
  }

  @Test
  void putItem_withConditionExpression_attributeNotExists_fails() {
    // Put an item first
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-2").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Original User").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Try to put again with condition that item doesn't exist - should fail
    final Map<String, AttributeValue> newItem = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-2").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Updated User").build()
    );

    final PutItemRequest conditionalPutRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(newItem)
        .conditionExpression("attribute_not_exists(userId)")
        .build();

    assertThatThrownBy(() -> client.putItem(conditionalPutRequest))
        .isInstanceOf(ConditionalCheckFailedException.class)
        .hasMessageContaining("conditional request failed");

    // Verify original item still exists
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("name").s()).isEqualTo("Original User");
  }

  @Test
  void putItem_withConditionExpression_attributeEquals_success() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-3").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("pending").build(),
        "version", AttributeValue.builder().n("1").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Update with condition that status = pending
    final Map<String, AttributeValue> updatedItem = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-3").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("active").build(),
        "version", AttributeValue.builder().n("2").build()
    );

    final PutItemRequest conditionalPutRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(updatedItem)
        .conditionExpression("#s = :pending AND #v = :version")
        .expressionAttributeNames(Map.of("#s", "status", "#v", "version"))
        .expressionAttributeValues(Map.of(
            ":pending", AttributeValue.builder().s("pending").build(),
            ":version", AttributeValue.builder().n("1").build()
        ))
        .build();

    final PutItemResponse putResponse = client.putItem(conditionalPutRequest);
    assertThat(putResponse).isNotNull();

    // Verify update succeeded
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-user-3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("status").s()).isEqualTo("active");
    assertThat(getResponse.item().get("version").n()).isEqualTo("2");
  }

  @Test
  void deleteItem_withConditionExpression_attributeExists_success() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("del-user-1").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("To Delete").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete with condition that item exists
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("attribute_exists(userId)")
        .returnValues(ReturnValue.ALL_OLD)
        .build();

    final DeleteItemResponse deleteResponse = client.deleteItem(deleteRequest);
    assertThat(deleteResponse.attributes()).isNotNull();
    assertThat(deleteResponse.attributes().get("name").s()).isEqualTo("To Delete");

    // Verify item was deleted
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void deleteItem_withConditionExpression_attributeExists_fails() {
    // Try to delete non-existent item with condition - should fail
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("nonexistent-user").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("attribute_exists(userId)")
        .build();

    assertThatThrownBy(() -> client.deleteItem(deleteRequest))
        .isInstanceOf(ConditionalCheckFailedException.class)
        .hasMessageContaining("conditional request failed");
  }

  @Test
  void deleteItem_withConditionExpression_attributeEquals_success() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("del-user-2").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("archived").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete only if status = archived
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("#s = :archived")
        .expressionAttributeNames(Map.of("#s", "status"))
        .expressionAttributeValues(Map.of(
            ":archived", AttributeValue.builder().s("archived").build()
        ))
        .build();

    final DeleteItemResponse deleteResponse = client.deleteItem(deleteRequest);
    assertThat(deleteResponse).isNotNull();

    // Verify item was deleted
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void deleteItem_withConditionExpression_attributeEquals_fails() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("del-user-3").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("active").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Try to delete with wrong status - should fail
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("#s = :archived")
        .expressionAttributeNames(Map.of("#s", "status"))
        .expressionAttributeValues(Map.of(
            ":archived", AttributeValue.builder().s("archived").build()
        ))
        .build();

    assertThatThrownBy(() -> client.deleteItem(deleteRequest))
        .isInstanceOf(ConditionalCheckFailedException.class)
        .hasMessageContaining("conditional request failed");

    // Verify item still exists
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("status").s()).isEqualTo("active");
  }

  @Test
  void batchGetItem_retrievesMultipleItems() {
    // Put some items first
    for (int i = 0; i < 3; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s("batch-user-" + i).build(),
              SORT_KEY, AttributeValue.builder().s("2024-01-0" + (i + 1) + "T00:00:00Z").build(),
              "name", AttributeValue.builder().s("Batch User " + i).build()
          ))
          .build());
    }

    // Batch get items
    final software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse response = client.batchGetItem(
        software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes.builder()
                    .keys(
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-user-0").build(),
                            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
                        ),
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-user-1").build(),
                            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build()
                        ),
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-user-2").build(),
                            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build()
                        )
                    )
                    .build()
            ))
            .build()
    );

    assertThat(response.responses()).containsKey(TABLE_NAME);
    assertThat(response.responses().get(TABLE_NAME)).hasSize(3);
    assertThat(response.responses().get(TABLE_NAME).get(0)).containsKey("name");
  }

  @Test
  void batchWriteItem_putsAndDeletesMultipleItems() {
    // Put initial items to delete
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-del-user").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "name", AttributeValue.builder().s("To Delete").build()
        ))
        .build());

    // Batch write - put 2 new items and delete 1 existing item
    client.batchWriteItem(
        software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, List.of(
                    // Put request 1
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                            .item(Map.of(
                                HASH_KEY, AttributeValue.builder().s("batch-write-user-1").build(),
                                SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
                                "name", AttributeValue.builder().s("Batch Write User 1").build()
                            ))
                            .build())
                        .build(),
                    // Put request 2
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                            .item(Map.of(
                                HASH_KEY, AttributeValue.builder().s("batch-write-user-2").build(),
                                SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
                                "name", AttributeValue.builder().s("Batch Write User 2").build()
                            ))
                            .build())
                        .build(),
                    // Delete request
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .deleteRequest(software.amazon.awssdk.services.dynamodb.model.DeleteRequest.builder()
                            .key(Map.of(
                                HASH_KEY, AttributeValue.builder().s("batch-del-user").build(),
                                SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
                            ))
                            .build())
                        .build()
                )
            ))
            .build()
    );

    // Verify the puts
    final GetItemResponse getResponse1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-write-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse1.hasItem()).isTrue();
    assertThat(getResponse1.item().get("name").s()).isEqualTo("Batch Write User 1");

    final GetItemResponse getResponse2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-write-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse2.hasItem()).isTrue();
    assertThat(getResponse2.item().get("name").s()).isEqualTo("Batch Write User 2");

    // Verify the delete
    final GetItemResponse getResponseDel = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-del-user").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponseDel.hasItem()).isFalse();
  }

  @Test
  void query_withFilterExpression_filtersResults() {
    // Put multiple items
    final String hashKey = "user123";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "age", AttributeValue.builder().n("25").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build(),
            "status", AttributeValue.builder().s("inactive").build(),
            "age", AttributeValue.builder().n("30").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "age", AttributeValue.builder().n("35").build()
        ))
        .build());

    // Query with FilterExpression to only get active users
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .filterExpression("#status = :statusVal")
        .expressionAttributeNames(Map.of(
            "#pk", HASH_KEY,
            "#status", "status"
        ))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":statusVal", AttributeValue.builder().s("active").build()
        ))
        .build());

    // Should only return 2 items (the active ones)
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).allMatch(item -> item.get("status").s().equals("active"));
  }

  @Test
  void query_withFilterExpression_comparisonOperators() {
    // Put items with numeric attributes
    final String hashKey = "user456";

    for (int i = 1; i <= 5; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s("2024-01-0" + i + "T00:00:00Z").build(),
              "score", AttributeValue.builder().n(String.valueOf(i * 10)).build()
          ))
          .build());
    }

    // Query with FilterExpression using comparison operator
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :pk")
        .filterExpression("score > :minScore")
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":minScore", AttributeValue.builder().n("30").build()
        ))
        .build());

    // Should return items with score > 30 (40 and 50)
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).allMatch(item ->
        Integer.parseInt(item.get("score").n()) > 30);
  }

  @Test
  void scan_withFilterExpression_filtersResults() {
    // Put multiple items
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "category", AttributeValue.builder().s("premium").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build(),
            "category", AttributeValue.builder().s("basic").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "category", AttributeValue.builder().s("premium").build()
        ))
        .build());

    // Scan with FilterExpression
    final ScanResponse response = client.scan(ScanRequest.builder()
        .tableName(TABLE_NAME)
        .filterExpression("category = :cat")
        .expressionAttributeValues(Map.of(
            ":cat", AttributeValue.builder().s("premium").build()
        ))
        .build());

    // Should only return 2 premium items
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).allMatch(item -> item.get("category").s().equals("premium"));
  }

  @Test
  void scan_withFilterExpression_attributeFunctions() {
    // Put items with/without certain attributes
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "email", AttributeValue.builder().s("user1@example.com").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build()
            // No email attribute
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "email", AttributeValue.builder().s("user3@example.com").build()
        ))
        .build());

    // Scan with FilterExpression using attribute_exists
    final ScanResponse response = client.scan(ScanRequest.builder()
        .tableName(TABLE_NAME)
        .filterExpression("attribute_exists(email)")
        .build());

    // Should only return 2 items with email attribute
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).allMatch(item -> item.containsKey("email"));
  }

  @Test
  void query_withFilterExpression_complexExpression() {
    // Put items
    final String hashKey = "user789";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "score", AttributeValue.builder().n("80").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "score", AttributeValue.builder().n("40").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "status", AttributeValue.builder().s("inactive").build(),
            "score", AttributeValue.builder().n("90").build()
        ))
        .build());

    // Query with complex FilterExpression (AND condition)
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :pk")
        .filterExpression("#status = :statusVal AND score >= :minScore")
        .expressionAttributeNames(Map.of("#status", "status"))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":statusVal", AttributeValue.builder().s("active").build(),
            ":minScore", AttributeValue.builder().n("50").build()
        ))
        .build());

    // Should only return 1 item (active with score >= 50)
    assertThat(response.count()).isEqualTo(1);
    assertThat(response.items()).hasSize(1);
    assertThat(response.items().get(0).get("score").n()).isEqualTo("80");
  }

  @Test
  void transactGetItems_success() {
    // Put test items
    final String hashKey1 = "txn-get-1";
    final String hashKey2 = "txn-get-2";
    final String sortKey1 = "item-1";
    final String sortKey2 = "item-2";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
            SORT_KEY, AttributeValue.builder().s(sortKey1).build(),
            "data", AttributeValue.builder().s("first item").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
            SORT_KEY, AttributeValue.builder().s(sortKey2).build(),
            "data", AttributeValue.builder().s("second item").build()
        ))
        .build());

    // Execute transactGetItems
    final software.amazon.awssdk.services.dynamodb.model.TransactGetItemsResponse response =
        client.transactGetItems(software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest.builder()
            .transactItems(
                software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
                    .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
                            SORT_KEY, AttributeValue.builder().s(sortKey1).build()
                        ))
                        .build())
                    .build(),
                software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
                    .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
                            SORT_KEY, AttributeValue.builder().s(sortKey2).build()
                        ))
                        .build())
                    .build()
            )
            .build());

    // Verify both items were retrieved
    assertThat(response.responses()).hasSize(2);
    assertThat(response.responses().get(0).item().get("data").s()).isEqualTo("first item");
    assertThat(response.responses().get(1).item().get("data").s()).isEqualTo("second item");
  }

  @Test
  void transactWriteItems_allPuts_success() {
    final String hashKey1 = "txn-put-1";
    final String hashKey2 = "txn-put-2";

    // Execute transactWriteItems with two puts
    client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
        .transactItems(
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                    .tableName(TABLE_NAME)
                    .item(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
                        SORT_KEY, AttributeValue.builder().s("item-1").build(),
                        "value", AttributeValue.builder().n("100").build()
                    ))
                    .build())
                .build(),
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                    .tableName(TABLE_NAME)
                    .item(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
                        SORT_KEY, AttributeValue.builder().s("item-2").build(),
                        "value", AttributeValue.builder().n("200").build()
                    ))
                    .build())
                .build()
        )
        .build());

    // Verify both items were created
    final GetItemResponse item1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build()
        ))
        .build());

    final GetItemResponse item2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
            SORT_KEY, AttributeValue.builder().s("item-2").build()
        ))
        .build());

    assertThat(item1.item().get("value").n()).isEqualTo("100");
    assertThat(item2.item().get("value").n()).isEqualTo("200");
  }

  @Test
  void transactWriteItems_mixedOperations_success() {
    final String hashKey = "txn-mixed";

    // First create an item to update and delete
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-update").build(),
            "value", AttributeValue.builder().n("10").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-delete").build(),
            "value", AttributeValue.builder().n("20").build()
        ))
        .build());

    // Execute transaction with Put, Update, and Delete
    client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
        .transactItems(
            // Put a new item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                    .tableName(TABLE_NAME)
                    .item(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-new").build(),
                        "value", AttributeValue.builder().n("30").build()
                    ))
                    .build())
                .build(),
            // Update existing item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .update(software.amazon.awssdk.services.dynamodb.model.Update.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-update").build()
                    ))
                    .updateExpression("SET #v = #v + :inc")
                    .expressionAttributeNames(Map.of("#v", "value"))
                    .expressionAttributeValues(Map.of(
                        ":inc", AttributeValue.builder().n("5").build()
                    ))
                    .build())
                .build(),
            // Delete existing item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .delete(software.amazon.awssdk.services.dynamodb.model.Delete.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-delete").build()
                    ))
                    .build())
                .build()
        )
        .build());

    // Verify new item was created
    final GetItemResponse newItem = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-new").build()
        ))
        .build());
    assertThat(newItem.item().get("value").n()).isEqualTo("30");

    // Verify update was applied
    final GetItemResponse updatedItem = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-update").build()
        ))
        .build());
    assertThat(updatedItem.item().get("value").n()).isEqualTo("15");

    // Verify item was deleted
    final GetItemResponse deletedItem = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-delete").build()
        ))
        .build());
    assertThat(deletedItem.hasItem()).isFalse();
  }

  @Test
  void transactWriteItems_conditionCheckSuccess() {
    final String hashKey = "txn-condition";

    // Create item with version attribute
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build(),
            "version", AttributeValue.builder().n("1").build(),
            "data", AttributeValue.builder().s("original").build()
        ))
        .build());

    // Execute transaction with condition check and update
    client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
        .transactItems(
            // Condition check - verify version is 1
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .conditionCheck(software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-1").build()
                    ))
                    .conditionExpression("version = :v")
                    .expressionAttributeValues(Map.of(
                        ":v", AttributeValue.builder().n("1").build()
                    ))
                    .build())
                .build(),
            // Update item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .update(software.amazon.awssdk.services.dynamodb.model.Update.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-1").build()
                    ))
                    .updateExpression("SET version = :newV, #d = :newData")
                    .expressionAttributeNames(Map.of("#d", "data"))
                    .expressionAttributeValues(Map.of(
                        ":newV", AttributeValue.builder().n("2").build(),
                        ":newData", AttributeValue.builder().s("updated").build()
                    ))
                    .build())
                .build()
        )
        .build());

    // Verify update was applied
    final GetItemResponse item = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build()
        ))
        .build());

    assertThat(item.item().get("version").n()).isEqualTo("2");
    assertThat(item.item().get("data").s()).isEqualTo("updated");
  }

  @Test
  void transactWriteItems_conditionCheckFailure() {
    final String hashKey = "txn-fail";

    // Create item with version attribute
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build(),
            "version", AttributeValue.builder().n("2").build()
        ))
        .build());

    // Attempt transaction with incorrect version check - should fail
    assertThatThrownBy(() ->
        client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
            .transactItems(
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .conditionCheck(software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("item-1").build()
                        ))
                        .conditionExpression("version = :v")
                        .expressionAttributeValues(Map.of(
                            ":v", AttributeValue.builder().n("1").build()  // Wrong version!
                        ))
                        .build())
                    .build()
            )
            .build())
    ).isInstanceOf(software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException.class);
  }
}
