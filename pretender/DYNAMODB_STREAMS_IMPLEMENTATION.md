# DynamoDB Streams Implementation Plan

## Overview

Implement DynamoDB Streams support in the pretender module to capture and provide access to item-level changes (INSERT, MODIFY, REMOVE events) in a DynamoDB-compatible manner using SQL database storage.

**Status**: Planning Phase
**Estimated Effort**: 6-9 days
**Target Completion**: TBD

---

## Goals

### Primary Objectives
1. ✅ Capture all item changes (INSERT, MODIFY, REMOVE) when stream is enabled
2. ✅ Provide DynamoDB Streams API compatibility for consuming change records
3. ✅ Support all StreamViewType configurations (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES)
4. ✅ Implement automatic cleanup of stream records after 24 hours (matching AWS behavior)
5. ✅ Maintain atomicity between item changes and stream record creation

### Non-Goals (Deferred)
- ❌ Multiple shards per table (use single shard for simplicity)
- ❌ Distributed sequence numbers (use database auto-increment)
- ❌ Lambda triggers (out of scope for pretender)
- ❌ Cross-region replication (not applicable)

---

## Architecture

### Storage Model

Each DynamoDB table with streams enabled gets a corresponding stream table:

```sql
CREATE TABLE pdb_stream_<tablename> (
  sequence_number BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(128) NOT NULL UNIQUE,    -- UUID for idempotency
  event_type VARCHAR(16) NOT NULL,          -- INSERT, MODIFY, REMOVE
  event_timestamp TIMESTAMP NOT NULL,
  hash_key_value VARCHAR(2048) NOT NULL,
  sort_key_value VARCHAR(2048),
  keys_json JSONB/CLOB NOT NULL,            -- Always present: primary keys
  old_image_json JSONB/CLOB,                -- Before change (nullable based on view type)
  new_image_json JSONB/CLOB,                -- After change (nullable based on view type)
  approximate_creation_time BIGINT NOT NULL, -- Epoch milliseconds
  size_bytes INT NOT NULL,                   -- Approximate record size
  create_date TIMESTAMP NOT NULL,
  INDEX idx_timestamp (event_timestamp),
  INDEX idx_sequence (sequence_number)
);
```

**Key Design Choices**:
- `sequence_number`: Auto-increment provides strict ordering within a table
- `event_id`: UUID ensures idempotency if same event written multiple times
- `keys_json`: Always stored regardless of view type (required by AWS Streams API)
- `old_image_json` / `new_image_json`: Conditionally populated based on StreamViewType
- `approximate_creation_time`: Matches DynamoDB field name
- `size_bytes`: For compatibility with AWS API response

### Component Architecture

```
DynamoDbStreamsClient (AWS Streams SDK interface)
  └─> PdbStreamManager (Business logic)
      ├─> PdbStreamDao (SQL operations with JDBI)
      ├─> PdbTableManager (Stream enable/disable)
      ├─> StreamRecordConverter (Request/response conversion)
      └─> StreamCleanupService (Background 24-hour cleanup)

PdbItemManager (Existing - Enhanced)
  └─> StreamCaptureHelper (Capture changes during item operations)
      └─> PdbStreamDao (Write stream records)
```

### Stream Metadata Storage

Extend existing `pdb_metadata` table:

```sql
ALTER TABLE pdb_metadata ADD COLUMN stream_enabled BOOLEAN DEFAULT FALSE;
ALTER TABLE pdb_metadata ADD COLUMN stream_view_type VARCHAR(32);
ALTER TABLE pdb_metadata ADD COLUMN stream_arn VARCHAR(512);
ALTER TABLE pdb_metadata ADD COLUMN stream_label VARCHAR(128);
```

---

## Detailed Implementation Plan

### Phase 1: Foundation - Models & Storage (2-3 days)

#### 1.1 Create Stream Record Model

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/model/PdbStreamRecord.java`

```java
@Value.Immutable
@JsonSerialize(as = ImmutablePdbStreamRecord.class)
@JsonDeserialize(as = ImmutablePdbStreamRecord.class)
public interface PdbStreamRecord {
  long sequenceNumber();
  String eventId();
  String eventType();              // INSERT, MODIFY, REMOVE
  Instant eventTimestamp();
  String hashKeyValue();
  Optional<String> sortKeyValue();
  String keysJson();               // Always present
  Optional<String> oldImageJson(); // Conditional on view type
  Optional<String> newImageJson(); // Conditional on view type
  long approximateCreationTime();  // Epoch milliseconds
  int sizeBytes();
  Instant createDate();
}
```

**Test**: `PdbStreamRecordTest.java` - Test immutables builder and JSON serialization

#### 1.2 Create Stream Metadata Model

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/model/PdbStreamMetadata.java`

```java
@Value.Immutable
@JsonSerialize(as = ImmutablePdbStreamMetadata.class)
@JsonDeserialize(as = ImmutablePdbStreamMetadata.class)
public interface PdbStreamMetadata {
  String tableName();
  boolean streamEnabled();
  Optional<StreamViewType> streamViewType();
  Optional<String> streamArn();
  Optional<String> streamLabel();
  Optional<Instant> creationDateTime();
}
```

#### 1.3 Update PdbMetadata Model

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/model/PdbMetadata.java` (MODIFY)

Add stream-related fields:
```java
Optional<Boolean> streamEnabled();
Optional<String> streamViewType();
Optional<String> streamArn();
Optional<String> streamLabel();
```

#### 1.4 Create Liquibase Migration

**File**: `pretender/src/main/resources/liquibase/db-003.xml`

```xml
<changeSet id="add-stream-columns" author="claude">
  <addColumn tableName="pdb_metadata">
    <column name="stream_enabled" type="BOOLEAN" defaultValueBoolean="false"/>
    <column name="stream_view_type" type="VARCHAR(32)"/>
    <column name="stream_arn" type="VARCHAR(512)"/>
    <column name="stream_label" type="VARCHAR(128)"/>
  </addColumn>
</changeSet>
```

**File**: `pretender/src/main/resources/liquibase/liquibase-setup.xml` (MODIFY)

Add reference to db-003.xml

#### 1.5 Create PdbStreamTableManager

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/manager/PdbStreamTableManager.java`

**Purpose**: Manage dynamic stream table creation/deletion

**Key methods**:
- `void createStreamTable(String tableName)` - Create pdb_stream_<name> table
- `void dropStreamTable(String tableName)` - Drop stream table
- `String getStreamTableName(String dynamoTableName)` - Naming convention
- Database-aware SQL (PostgreSQL vs HSQLDB for auto-increment)

**Test**: `PdbStreamTableManagerTest.java` - DDL operations, idempotency

#### 1.6 Create PdbStreamDao

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/dao/PdbStreamDao.java`

**Purpose**: CRUD operations for stream records

**Key methods**:
```java
boolean insert(String tableName, PdbStreamRecord record);
List<PdbStreamRecord> getRecords(String tableName, long startSequence, int limit);
long getLatestSequenceNumber(String tableName);
long getTrimHorizon(String tableName);  // Oldest available sequence
int deleteOlderThan(String tableName, Instant cutoffTime);
```

**Test**: `PdbStreamDaoTest.java` - Extends BaseJdbiTest

#### 1.7 Update PdbMetadataDao

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/dao/PdbMetadataDao.java` (MODIFY)

Add stream configuration methods:
```java
boolean updateStreamConfig(String tableName, boolean enabled, String viewType, String arn, String label);
```

#### 1.8 Update PretenderModule

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/dagger/PretenderModule.java` (MODIFY)

Add PdbStreamRecord to immutables set:
```java
@Provides
@Singleton
@Named(JdbiFactory.IMMUTABLES)
public Set<Class<?>> immutableClasses() {
  return Set.of(PdbMetadata.class, PdbItem.class, PdbStreamRecord.class);
}
```

---

### Phase 2: Change Capture (2-3 days)

#### 2.1 Create StreamCaptureHelper

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/helper/StreamCaptureHelper.java`

**Purpose**: Encapsulate logic for capturing item changes to streams

**Key methods**:
```java
void captureInsert(String tableName, Map<String, AttributeValue> newItem);
void captureModify(String tableName,
                   Map<String, AttributeValue> oldItem,
                   Map<String, AttributeValue> newItem);
void captureRemove(String tableName, Map<String, AttributeValue> oldItem);
```

**Implementation**:
- Check if stream enabled for table
- Get StreamViewType configuration
- Build PdbStreamRecord based on event type and view type
- Calculate approximate size
- Insert via PdbStreamDao

**Test**: `StreamCaptureHelperTest.java` - Mock dependencies, verify correct records created

#### 2.2 Enhance PdbItemManager - putItem

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/manager/PdbItemManager.java` (MODIFY)

**Changes**:
```java
public PutItemResponse putItem(final PutItemRequest request) {
  // ... existing logic ...

  // Capture stream event BEFORE actual write (to get old item if exists)
  if (existingPdbItem.isPresent()) {
    // MODIFY event
    streamCaptureHelper.captureModify(
      tableName,
      attributeValueConverter.fromJson(existingPdbItem.get().attributesJson()),
      request.item()
    );
  } else {
    // INSERT event
    streamCaptureHelper.captureInsert(tableName, request.item());
  }

  // Perform actual write (insert or update)
  // ... existing logic ...
}
```

#### 2.3 Enhance PdbItemManager - deleteItem

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/manager/PdbItemManager.java` (MODIFY)

**Changes**:
```java
public DeleteItemResponse deleteItem(final DeleteItemRequest request) {
  // ... get old item ...

  // Capture REMOVE event BEFORE actual delete
  if (oldItem != null) {
    streamCaptureHelper.captureRemove(tableName, oldItem);
  }

  // Perform actual delete
  // ... existing logic ...
}
```

#### 2.4 Enhance PdbItemManager - updateItem

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/manager/PdbItemManager.java` (MODIFY)

**Changes**:
```java
public UpdateItemResponse updateItem(final UpdateItemRequest request) {
  // ... get old item, apply update expression ...

  // Capture MODIFY event (or INSERT if item didn't exist)
  if (oldItem != null && !oldItem.isEmpty()) {
    streamCaptureHelper.captureModify(tableName, oldItem, updatedItem);
  } else {
    streamCaptureHelper.captureInsert(tableName, updatedItem);
  }

  // Perform actual write
  // ... existing logic ...
}
```

#### 2.5 Update PdbTableManager

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/manager/PdbTableManager.java` (MODIFY)

Add stream management methods:
```java
public void enableStream(String tableName, StreamViewType viewType) {
  // 1. Create stream table if doesn't exist
  streamTableManager.createStreamTable(tableName);

  // 2. Generate stream ARN and label
  String arn = generateStreamArn(tableName);
  String label = generateStreamLabel();

  // 3. Update metadata
  metadataDao.updateStreamConfig(tableName, true, viewType.name(), arn, label);
}

public void disableStream(String tableName) {
  metadataDao.updateStreamConfig(tableName, false, null, null, null);
  // Note: Don't drop stream table - records should persist for 24 hours
}
```

**Test**: Update `PdbTableManagerTest.java` with stream enable/disable tests

---

### Phase 3: Consumption API (2-3 days)

#### 3.1 Create Shard Iterator Model

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/model/ShardIterator.java`

```java
@Value.Immutable
public interface ShardIterator {
  String tableName();
  String shardId();           // Always "shard-00000" for pretender
  long sequenceNumber();
  ShardIteratorType type();   // TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, etc.
}
```

**Helper**: Encoding/decoding shard iterator to/from Base64 string

#### 3.2 Create StreamRecordConverter

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/converter/StreamRecordConverter.java`

**Purpose**: Convert between PdbStreamRecord and DynamoDB Streams API objects

**Key methods**:
```java
Record toStreamRecord(PdbStreamRecord pdbRecord);
StreamRecord toStreamRecordData(PdbStreamRecord pdbRecord);
```

**Test**: `StreamRecordConverterTest.java`

#### 3.3 Create PdbStreamManager

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/manager/PdbStreamManager.java`

**Purpose**: Business logic for stream operations

**Key methods**:

```java
public DescribeStreamResponse describeStream(DescribeStreamRequest request) {
  // Return stream metadata, shard information
  // Pretender uses single shard per table
}

public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest request) {
  // Encode iterator based on type:
  // - TRIM_HORIZON: Oldest available record
  // - LATEST: After last available record
  // - AT_SEQUENCE_NUMBER: Specific sequence
  // - AFTER_SEQUENCE_NUMBER: After specific sequence
}

public GetRecordsResponse getRecords(GetRecordsRequest request) {
  // 1. Decode shard iterator
  // 2. Fetch records from DAO
  // 3. Convert to Stream API format
  // 4. Create next iterator
  // 5. Return response
}

public ListStreamsResponse listStreams(ListStreamsRequest request) {
  // List all tables with streams enabled
}
```

**Test**: `PdbStreamManagerTest.java` - Mock dependencies

#### 3.4 Create DynamoDbStreamsPretenderClient

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/DynamoDbStreamsPretenderClient.java`

**Purpose**: Implement DynamoDbStreamsClient interface

```java
@Singleton
public class DynamoDbStreamsPretenderClient implements DynamoDbStreamsClient {

  private final PdbStreamManager streamManager;

  @Inject
  public DynamoDbStreamsPretenderClient(PdbStreamManager streamManager) {
    this.streamManager = streamManager;
  }

  @Override
  public DescribeStreamResponse describeStream(DescribeStreamRequest request) {
    return streamManager.describeStream(request);
  }

  @Override
  public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest request) {
    return streamManager.getShardIterator(request);
  }

  @Override
  public GetRecordsResponse getRecords(GetRecordsRequest request) {
    return streamManager.getRecords(request);
  }

  @Override
  public ListStreamsResponse listStreams(ListStreamsRequest request) {
    return streamManager.listStreams(request);
  }

  // Other required interface methods with UnsupportedOperationException
  // (e.g., serviceName(), close(), etc.)
}
```

#### 3.5 Update PretenderComponent

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/component/PretenderComponent.java` (MODIFY)

```java
@Singleton
@Component(modules = {PretenderModule.class, DatabaseModule.class})
public interface PretenderComponent {
  // ... existing methods ...

  DynamoDbStreamsPretenderClient dynamoDbStreamsPretenderClient();
}
```

---

### Phase 4: Cleanup & Maintenance (1-2 days)

#### 4.1 Create StreamCleanupService

**File**: `pretender/src/main/java/com/codeheadsystems/pretender/service/StreamCleanupService.java`

**Purpose**: Background service to delete stream records older than 24 hours

**Implementation** (similar to TtlCleanupService):
```java
@Singleton
public class StreamCleanupService {
  private final ScheduledExecutorService scheduler;
  private final PdbStreamDao streamDao;
  private final PdbMetadataDao metadataDao;

  public void start() {
    scheduler.scheduleAtFixedRate(
      this::cleanupExpiredRecords,
      0,
      60,  // Run every 60 minutes
      TimeUnit.MINUTES
    );
  }

  private void cleanupExpiredRecords() {
    // For each table with streams enabled
    // Delete records older than 24 hours
    Instant cutoff = Instant.now().minus(24, ChronoUnit.HOURS);
    List<String> streamTables = metadataDao.getTablesWithStreamsEnabled();

    for (String tableName : streamTables) {
      int deleted = streamDao.deleteOlderThan(tableName, cutoff);
      log.debug("Deleted {} expired stream records from {}", deleted, tableName);
    }
  }
}
```

**Test**: `StreamCleanupServiceTest.java`

---

### Phase 5: Testing (2-3 days)

#### 5.1 Unit Tests

Create comprehensive unit tests for all new components:
- `PdbStreamRecordTest.java` - Model tests
- `PdbStreamDaoTest.java` - Database operations
- `PdbStreamTableManagerTest.java` - DDL operations
- `StreamCaptureHelperTest.java` - Event capture logic
- `StreamRecordConverterTest.java` - Conversion tests
- `PdbStreamManagerTest.java` - Business logic
- `StreamCleanupServiceTest.java` - Cleanup logic

#### 5.2 Integration Tests

**File**: `pretender/src/test/java/com/codeheadsystems/pretender/endToEnd/StreamsTest.java`

**Test scenarios**:
```java
@Test
void putItem_withStreamEnabled_capturesInsertEvent() {
  // Enable stream with NEW_AND_OLD_IMAGES
  tableManager.enableStream(TABLE_NAME, StreamViewType.NEW_AND_OLD_IMAGES);

  // Put new item
  client.putItem(PutItemRequest.builder()
    .tableName(TABLE_NAME)
    .item(testItem)
    .build());

  // Read from stream
  GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
    GetShardIteratorRequest.builder()
      .streamArn(getStreamArn(TABLE_NAME))
      .shardId("shard-00000")
      .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
      .build()
  );

  GetRecordsResponse records = streamsClient.getRecords(
    GetRecordsRequest.builder()
      .shardIterator(iteratorResponse.shardIterator())
      .build()
  );

  assertThat(records.records()).hasSize(1);
  Record record = records.records().get(0);
  assertThat(record.eventName()).isEqualTo("INSERT");
  assertThat(record.dynamodb().newImage()).isNotNull();
  assertThat(record.dynamodb().oldImage()).isNull();
}

@Test
void updateItem_withStreamEnabled_capturesModifyEvent() {
  // Enable stream, put item, update item
  // Verify MODIFY event with both old and new images
}

@Test
void deleteItem_withStreamEnabled_capturesRemoveEvent() {
  // Enable stream, put item, delete item
  // Verify REMOVE event with old image
}

@Test
void streamViewType_keysOnly_onlyIncludesKeys() {
  // Enable stream with KEYS_ONLY
  // Verify records only contain keys, no images
}

@Test
void getRecords_withLimit_respectsLimit() {
  // Insert 10 items
  // Get records with limit=5
  // Verify 5 records returned
  // Verify next iterator works
}

@Test
void cleanupService_deletesRecordsOlderThan24Hours() {
  // Insert old records (timestamp manipulated)
  // Run cleanup
  // Verify old records deleted, recent ones remain
}

@Test
void multipleEvents_maintainStrictOrdering() {
  // Perform multiple operations
  // Verify sequence numbers are strictly increasing
}

@Test
void disableStream_stopsCapturingNewEvents() {
  // Enable stream, insert item (captured)
  // Disable stream, insert item (not captured)
  // Verify only 1 stream record exists
}
```

#### 5.3 End-to-End Workflow Test

**File**: `pretender/src/test/java/com/codeheadsystems/pretender/endToEnd/StreamsWorkflowTest.java`

```java
@Test
void completeStreamWorkflow() {
  // 1. Create table
  // 2. Enable stream with NEW_AND_OLD_IMAGES
  // 3. Insert 3 items (3 INSERT events)
  // 4. Update 2 items (2 MODIFY events)
  // 5. Delete 1 item (1 REMOVE event)
  // 6. Consume all 6 events from stream
  // 7. Verify event types, images, sequence ordering
  // 8. Verify pagination works with small limits
}
```

---

## Implementation Checklist (TODO List)

### Phase 1: Foundation - Models & Storage
- [ ] Create `PdbStreamRecord.java` model with Immutables
- [ ] Create `PdbStreamRecordTest.java` unit test
- [ ] Create `PdbStreamMetadata.java` model
- [ ] Update `PdbMetadata.java` with stream fields
- [ ] Create Liquibase migration `db-003.xml` for stream columns
- [ ] Update `liquibase-setup.xml` to include db-003
- [ ] Create `PdbStreamTableManager.java` for DDL operations
- [ ] Create `PdbStreamTableManagerTest.java` unit test
- [ ] Create `PdbStreamDao.java` for stream record CRUD
- [ ] Create `PdbStreamDaoTest.java` integration test
- [ ] Update `PdbMetadataDao.java` with stream config methods
- [ ] Update `PretenderModule.java` to include PdbStreamRecord in immutables
- [ ] Run tests to verify foundation layer works

### Phase 2: Change Capture
- [ ] Create `StreamCaptureHelper.java` for event capture logic
- [ ] Create `StreamCaptureHelperTest.java` unit test
- [ ] Enhance `PdbItemManager.putItem()` to capture INSERT/MODIFY events
- [ ] Enhance `PdbItemManager.deleteItem()` to capture REMOVE events
- [ ] Enhance `PdbItemManager.updateItem()` to capture MODIFY events
- [ ] Update `PdbTableManager.java` with enableStream/disableStream methods
- [ ] Update `PdbTableManagerTest.java` with stream enable/disable tests
- [ ] Test stream capture with manual verification (insert records and check stream table)
- [ ] Verify atomicity (stream record written in same transaction as item change)

### Phase 3: Consumption API
- [ ] Create `ShardIterator.java` model with encoder/decoder utilities
- [ ] Create `StreamRecordConverter.java` for API conversion
- [ ] Create `StreamRecordConverterTest.java` unit test
- [ ] Create `PdbStreamManager.java` with business logic
  - [ ] Implement `describeStream()`
  - [ ] Implement `getShardIterator()`
  - [ ] Implement `getRecords()`
  - [ ] Implement `listStreams()`
- [ ] Create `PdbStreamManagerTest.java` unit test
- [ ] Create `DynamoDbStreamsPretenderClient.java` implementing AWS Streams interface
- [ ] Update `PretenderComponent.java` to expose streams client
- [ ] Test shard iterator encoding/decoding
- [ ] Test record retrieval with different iterator types

### Phase 4: Cleanup & Maintenance
- [ ] Create `StreamCleanupService.java` for 24-hour cleanup
- [ ] Create `StreamCleanupServiceTest.java` unit test
- [ ] Add cleanup service to PretenderComponent
- [ ] Test cleanup service removes old records
- [ ] Test cleanup service runs on schedule

### Phase 5: Testing
- [ ] Create `StreamsTest.java` for end-to-end stream tests
  - [ ] Test INSERT event capture
  - [ ] Test MODIFY event capture
  - [ ] Test REMOVE event capture
  - [ ] Test KEYS_ONLY view type
  - [ ] Test NEW_IMAGE view type
  - [ ] Test OLD_IMAGE view type
  - [ ] Test NEW_AND_OLD_IMAGES view type
  - [ ] Test getRecords with limit and pagination
  - [ ] Test stream disable stops capturing
  - [ ] Test sequence number ordering
- [ ] Create `StreamsWorkflowTest.java` for complete workflow
- [ ] Run all pretender tests to ensure no regressions
- [ ] Verify test coverage > 90% for stream components
- [ ] Performance test: 1000 events capture and retrieval

### Phase 6: Documentation
- [ ] Update `IMPLEMENTATION_SUMMARY.md` with Streams section
- [ ] Add usage examples for stream enable/consume
- [ ] Update test count in summary
- [ ] Document limitations (single shard, etc.)
- [ ] Add troubleshooting guide
- [ ] Create migration guide for existing tables

---

## Success Criteria

✅ All StreamViewType configurations supported (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES)
✅ Stream records captured atomically with item changes
✅ GetRecords API returns properly formatted stream records
✅ Shard iterator types work correctly (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER)
✅ Sequence numbers maintain strict ordering
✅ Background cleanup removes records after 24 hours
✅ All unit tests passing (>30 new tests)
✅ All integration tests passing (>8 end-to-end tests)
✅ Zero regressions in existing tests
✅ Code coverage > 90% for stream components

---

## Key Design Principles

1. **Atomicity First**: Stream records must be written in the same transaction as item changes
2. **Simplicity Over Complexity**: Single shard per table is sufficient for local development/testing
3. **AWS API Compatibility**: Implement exact AWS DynamoDB Streams API responses
4. **Consistent Patterns**: Follow existing pretender architecture (Dagger, JDBI, Immutables)
5. **Test-Driven**: Write tests before implementation where possible
6. **Performance Conscious**: Index stream tables appropriately, background cleanup

---

## Risks & Mitigation

### Risk 1: Transaction Complexity
**Issue**: Writing stream records atomically with item changes requires proper transaction handling
**Mitigation**: Use JDBI's transaction support; all operations within same handle.inTransaction()

### Risk 2: Stream Table Size Growth
**Issue**: High-write tables could generate massive stream tables
**Mitigation**: Aggressive 24-hour cleanup, indexed by timestamp for efficient deletion

### Risk 3: Sequence Number Gaps
**Issue**: Failed transactions might create gaps in sequence numbers
**Mitigation**: Document that gaps are expected; consumers should handle missing sequences

### Risk 4: Multiple Database Compatibility
**Issue**: Auto-increment syntax differs between PostgreSQL and HSQLDB
**Mitigation**: Use PdbStreamTableManager with database detection (like PdbItemTableManager)

### Risk 5: Old Image Retrieval Performance
**Issue**: Getting old image for MODIFY events requires extra read
**Mitigation**: Already doing this for conditional expressions; minimal additional overhead

---

## Future Enhancements (Post-Initial Implementation)

- **Parallel Shard Support**: If needed, implement multiple shards per table
- **Stream Record Archival**: Archive to S3-compatible storage instead of deletion
- **Stream Metrics**: Track capture rate, consumption lag, record size
- **Change Data Capture Triggers**: Allow registration of callbacks for specific event patterns
- **Cross-Table Stream Aggregation**: Single stream endpoint for multiple tables

---

## Estimated Timeline

| Phase | Task | Estimated Days |
|-------|------|----------------|
| 1 | Foundation - Models & Storage | 2-3 days |
| 2 | Change Capture | 2 days |
| 3 | Consumption API | 2-3 days |
| 4 | Cleanup & Maintenance | 1 day |
| 5 | Testing | 2 days |
| 6 | Documentation | 0.5 days |
| **Total** | | **6-9 days** |

---

## Notes

- This implementation provides **change data capture** capabilities for local DynamoDB development
- Useful for testing Lambda triggers, data pipelines, and event-driven architectures locally
- Stream records are **ephemeral** (24-hour retention) matching AWS behavior
- **Not recommended** for production audit logging (use dedicated audit tables instead)
- Consider this a **development/testing tool**, not a production CDC solution
