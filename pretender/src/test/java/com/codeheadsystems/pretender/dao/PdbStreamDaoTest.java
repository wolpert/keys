package com.codeheadsystems.pretender.dao;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeheadsystems.pretender.BaseJdbiTest;
import com.codeheadsystems.pretender.manager.PdbStreamTableManager;
import com.codeheadsystems.pretender.model.ImmutablePdbStreamRecord;
import com.codeheadsystems.pretender.model.PdbStreamRecord;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PdbStreamDaoTest extends BaseJdbiTest {

  private static final String TEST_TABLE = "test-stream-table";
  private static final String STREAM_TABLE_NAME = "pdb_stream_test-stream-table";

  private PdbStreamDao dao;
  private PdbStreamTableManager tableManager;

  @BeforeEach
  void setup() {
    dao = new PdbStreamDao(jdbi);
    tableManager = new PdbStreamTableManager(jdbi, configuration.database());

    // Create the stream table for testing
    tableManager.createStreamTable(TEST_TABLE);
  }

  @Test
  void insert_createsRecord() {
    final PdbStreamRecord record = createTestRecord("event-1", "INSERT");

    final boolean result = dao.insert(STREAM_TABLE_NAME, record);

    assertThat(result).isTrue();
  }

  @Test
  void insert_autoGeneratesSequenceNumber() {
    final PdbStreamRecord record1 = createTestRecord("event-1", "INSERT");
    final PdbStreamRecord record2 = createTestRecord("event-2", "MODIFY");

    dao.insert(STREAM_TABLE_NAME, record1);
    dao.insert(STREAM_TABLE_NAME, record2);

    // Verify records have sequential sequence numbers
    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    assertThat(records).hasSize(2);
    assertThat(records.get(0).sequenceNumber()).isLessThan(records.get(1).sequenceNumber());
  }

  @Test
  void getRecords_returnsRecordsAfterSequence() {
    // Insert 5 records
    for (int i = 1; i <= 5; i++) {
      dao.insert(STREAM_TABLE_NAME, createTestRecord("event-" + i, "INSERT"));
    }

    // Get all records to find the 3rd sequence number
    final List<PdbStreamRecord> allRecords = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    final long thirdSequence = allRecords.get(2).sequenceNumber();

    // Get records after the 3rd
    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, thirdSequence, 10);

    assertThat(records).hasSize(2);
    assertThat(records.get(0).eventId()).isEqualTo("event-4");
    assertThat(records.get(1).eventId()).isEqualTo("event-5");
  }

  @Test
  void getRecords_respectsLimit() {
    // Insert 10 records
    for (int i = 1; i <= 10; i++) {
      dao.insert(STREAM_TABLE_NAME, createTestRecord("event-" + i, "INSERT"));
    }

    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 5);

    assertThat(records).hasSize(5);
  }

  @Test
  void getRecords_orderedBySequenceNumber() {
    // Insert records
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-1", "INSERT"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-2", "MODIFY"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-3", "REMOVE"));

    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 10);

    assertThat(records).hasSize(3);
    // Verify ascending order
    for (int i = 0; i < records.size() - 1; i++) {
      assertThat(records.get(i).sequenceNumber())
          .isLessThan(records.get(i + 1).sequenceNumber());
    }
  }

  @Test
  void getLatestSequenceNumber_returnsHighestSequence() {
    // Insert multiple records
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-1", "INSERT"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-2", "MODIFY"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-3", "REMOVE"));

    final long latestSequence = dao.getLatestSequenceNumber(STREAM_TABLE_NAME);

    // Get all records to verify it's the highest
    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    final long expectedMax = records.stream()
        .mapToLong(PdbStreamRecord::sequenceNumber)
        .max()
        .orElse(0);

    assertThat(latestSequence).isEqualTo(expectedMax);
  }

  @Test
  void getLatestSequenceNumber_returnsZeroWhenEmpty() {
    final long latestSequence = dao.getLatestSequenceNumber(STREAM_TABLE_NAME);

    assertThat(latestSequence).isZero();
  }

  @Test
  void getTrimHorizon_returnsLowestSequence() {
    // Insert multiple records
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-1", "INSERT"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-2", "MODIFY"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-3", "REMOVE"));

    final long trimHorizon = dao.getTrimHorizon(STREAM_TABLE_NAME);

    // Get all records to verify it's the lowest
    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    final long expectedMin = records.stream()
        .mapToLong(PdbStreamRecord::sequenceNumber)
        .min()
        .orElse(0);

    assertThat(trimHorizon).isEqualTo(expectedMin);
  }

  @Test
  void getTrimHorizon_returnsZeroWhenEmpty() {
    final long trimHorizon = dao.getTrimHorizon(STREAM_TABLE_NAME);

    assertThat(trimHorizon).isZero();
  }

  @Test
  void deleteOlderThan_removesOldRecords() {
    final Instant now = Instant.now();
    final Instant old = now.minus(25, ChronoUnit.HOURS);
    final Instant recent = now.minus(1, ChronoUnit.HOURS);

    // Insert old record
    dao.insert(STREAM_TABLE_NAME, createTestRecordWithTimestamp("event-1", "INSERT", old));

    // Insert recent records
    dao.insert(STREAM_TABLE_NAME, createTestRecordWithTimestamp("event-2", "MODIFY", recent));
    dao.insert(STREAM_TABLE_NAME, createTestRecordWithTimestamp("event-3", "REMOVE", now));

    // Delete records older than 24 hours
    final Instant cutoff = now.minus(24, ChronoUnit.HOURS);
    final int deleted = dao.deleteOlderThan(STREAM_TABLE_NAME, cutoff);

    assertThat(deleted).isEqualTo(1);

    // Verify only recent records remain
    final List<PdbStreamRecord> remaining = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    assertThat(remaining).hasSize(2);
    assertThat(remaining).extracting(PdbStreamRecord::eventId)
        .containsExactly("event-2", "event-3");
  }

  @Test
  void deleteOlderThan_returnsZeroWhenNoRecordsDeleted() {
    final Instant now = Instant.now();
    dao.insert(STREAM_TABLE_NAME, createTestRecordWithTimestamp("event-1", "INSERT", now));

    final Instant cutoff = now.minus(24, ChronoUnit.HOURS);
    final int deleted = dao.deleteOlderThan(STREAM_TABLE_NAME, cutoff);

    assertThat(deleted).isZero();
  }

  @Test
  void count_returnsCorrectCount() {
    // Insert 3 records
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-1", "INSERT"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-2", "MODIFY"));
    dao.insert(STREAM_TABLE_NAME, createTestRecord("event-3", "REMOVE"));

    final long count = dao.count(STREAM_TABLE_NAME);

    assertThat(count).isEqualTo(3);
  }

  @Test
  void count_returnsZeroWhenEmpty() {
    final long count = dao.count(STREAM_TABLE_NAME);

    assertThat(count).isZero();
  }

  @Test
  void insert_withOptionalFields() {
    final PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(0L) // Will be auto-generated
        .eventId("event-with-optionals")
        .eventType("MODIFY")
        .eventTimestamp(Instant.now())
        .hashKeyValue("hash-123")
        .sortKeyValue("sort-456") // Optional field
        .keysJson("{\"id\":{\"S\":\"hash-123\"}}")
        .oldImageJson("{\"name\":{\"S\":\"old\"}}") // Optional field
        .newImageJson("{\"name\":{\"S\":\"new\"}}") // Optional field
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(256)
        .createDate(Instant.now())
        .build();

    final boolean result = dao.insert(STREAM_TABLE_NAME, record);

    assertThat(result).isTrue();

    // Verify retrieval
    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    assertThat(records).hasSize(1);
    assertThat(records.get(0).sortKeyValue()).hasValue("sort-456");
    assertThat(records.get(0).oldImageJson()).hasValue("{\"name\":{\"S\":\"old\"}}");
    assertThat(records.get(0).newImageJson()).hasValue("{\"name\":{\"S\":\"new\"}}");
  }

  @Test
  void insert_withoutOptionalFields() {
    final PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(0L)
        .eventId("event-without-optionals")
        .eventType("INSERT")
        .eventTimestamp(Instant.now())
        .hashKeyValue("hash-789")
        // No sort key
        .keysJson("{\"id\":{\"S\":\"hash-789\"}}")
        // No old/new images
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(128)
        .createDate(Instant.now())
        .build();

    final boolean result = dao.insert(STREAM_TABLE_NAME, record);

    assertThat(result).isTrue();

    // Verify retrieval
    final List<PdbStreamRecord> records = dao.getRecords(STREAM_TABLE_NAME, 0, 10);
    assertThat(records).hasSize(1);
    assertThat(records.get(0).sortKeyValue()).isEmpty();
    assertThat(records.get(0).oldImageJson()).isEmpty();
    assertThat(records.get(0).newImageJson()).isEmpty();
  }

  private PdbStreamRecord createTestRecord(final String eventId, final String eventType) {
    return createTestRecordWithTimestamp(eventId, eventType, Instant.now());
  }

  private PdbStreamRecord createTestRecordWithTimestamp(final String eventId,
                                                         final String eventType,
                                                         final Instant timestamp) {
    return ImmutablePdbStreamRecord.builder()
        .sequenceNumber(0L) // Will be auto-generated
        .eventId(eventId)
        .eventType(eventType)
        .eventTimestamp(timestamp)
        .hashKeyValue("test-hash-key")
        .keysJson("{\"id\":{\"S\":\"test-hash-key\"}}")
        .approximateCreationTime(timestamp.toEpochMilli())
        .sizeBytes(100)
        .createDate(timestamp)
        .build();
  }
}
