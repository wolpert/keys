# Pretender TODO List

This document outlines potential improvements, bug fixes, and enhancements for the Pretender DynamoDB-compatible client.

---

## 🔴 Critical Issues

### 1. Transaction Atomicity Not Implemented
**Priority:** CRITICAL  
**File:** `PdbItemManager.java` (lines 916-975)

**Problem:**  
The `transactWriteItems()` method doesn't use actual database transactions. Each operation is committed individually using `jdbi.withHandle()`. If operation #3 of 5 fails, operations #1 and #2 have already been permanently committed to the database, violating DynamoDB's all-or-nothing transaction semantics.

**Impact:**
- Data corruption risk
- Violates DynamoDB API contract
- Not production-ready for critical workflows (e.g., financial transfers)

**Solution:**
```java
// Replace processTransactWriteItem loop with:
return jdbi.inTransaction(handle -> {
    for (TransactWriteItem item : request.transactItems()) {
        processTransactWriteItem(item, handle);
    }
    return TransactWriteItemsResponse.builder().build();
});
```

**Estimated Effort:** 2-4 hours

---

### 2. Batch Write Error Handling Missing
**Priority:** CRITICAL  
**File:** `PdbItemManager.java` (line 835)

**Problem:**  
`batchWriteItem()` comment states "no unprocessed items in this simple implementation". If a write fails partway through, there's no mechanism to:
- Track which items succeeded vs failed
- Return unprocessed items to the caller
- Handle partial failures gracefully

**Impact:**
- Silent data loss on errors
- No retry mechanism for failed items
- Violates DynamoDB API contract

**Solution:**
- Wrap in try-catch per item
- Collect failed items in `unprocessedItems` map
- Return in `BatchWriteItemResponse.builder().unprocessedItems(map).build()`

**Estimated Effort:** 3-4 hours

---

## 🟠 High Priority

### 3. Scan Pagination Not Implemented
**Priority:** HIGH  
**File:** `PdbItemManager.java` (line 514)

**Problem:**  
Comment explicitly states "ExclusiveStartKey is not supported in initial implementation". This means:
- Scan can only retrieve first page of results
- Large tables cannot be fully scanned
- Pagination through scan results is impossible

**Impact:**
- Cannot scan tables with >100 items (default limit)
- Missing core DynamoDB feature

**Solution:**
- Accept `ExclusiveStartKey` from request
- Convert to WHERE clause: `WHERE (hash_key, sort_key) > (?, ?)`
- Add to DAO scan method

**Estimated Effort:** 2-3 hours

---

### 4. Query Pagination May Not Work Correctly
**Priority:** HIGH  
**File:** `PdbItemManager.java` (query method)

**Problem:**  
Need to verify if `ExclusiveStartKey` is properly handled in query operations. If not, same issue as scan.

**Solution:**
- Review query implementation for ExclusiveStartKey handling
- Add WHERE clause filtering if missing
- Add integration test for multi-page query results

**Estimated Effort:** 2-3 hours

---

### 5. Transaction Item Count Not Validated
**Priority:** HIGH  
**File:** `PdbItemManager.java` (transactWriteItems, transactGetItems)

**Problem:**  
DynamoDB limits transactions to 25 items max. No validation enforces this, potentially allowing invalid operations.

**Impact:**
- Behavior diverges from real DynamoDB
- Tests may pass in Pretender but fail in production

**Solution:**
```java
if (request.transactItems().size() > 25) {
    throw ValidationException.builder()
        .message("Transaction request cannot contain more than 25 items")
        .build();
}
```

**Estimated Effort:** 30 minutes

---

### 6. Batch Operation Limits Not Validated
**Priority:** HIGH  
**File:** `PdbItemManager.java` (batchGetItem, batchWriteItem)

**Problem:**  
DynamoDB limits:
- BatchGetItem: 100 items max
- BatchWriteItem: 25 requests max

No validation enforces these limits.

**Solution:**
- Add validation at start of each batch method
- Throw `ValidationException` if exceeded

**Estimated Effort:** 30 minutes

---

### 7. Item Size Not Validated
**Priority:** HIGH  
**File:** `PdbItemManager.java` (putItem, updateItem)

**Problem:**  
DynamoDB has a 400KB item size limit. Pretender doesn't validate this, allowing oversized items that would fail in real DynamoDB.

**Solution:**
```java
private void validateItemSize(Map<String, AttributeValue> item) {
    String json = attributeValueConverter.toJson(item);
    if (json.length() > 400_000) {
        throw ItemSizeTooLargeException.builder()
            .message("Item size exceeds 400KB limit")
            .build();
    }
}
```

**Estimated Effort:** 1-2 hours

---

## 🟡 Medium Priority

### 8. Performance: GSI Maintenance Uses Individual Inserts
**Priority:** MEDIUM  
**File:** `PdbItemManager.java` (maintainGsiTables, line 644)

**Problem:**  
For each GSI, the code does:
```java
itemDao.insert(gsiTableName, gsiItem);
```

If a table has 5 GSIs, that's 5 separate database round-trips per item.

**Solution:**
- Batch all GSI inserts into single SQL statement
- Use JDBI batch API or multi-row INSERT
- Could reduce GSI update time by 80%+

**Estimated Effort:** 3-4 hours

---

### 9. Performance: BatchGetItem Does Individual Queries
**Priority:** MEDIUM  
**File:** `PdbItemManager.java` (batchGetItem, line 759-785)

**Problem:**  
BatchGetItem loops through items calling `getItem()` individually:
```java
for (Map<String, AttributeValue> key : keysAndAttributes.keys()) {
    final GetItemResponse getResponse = getItem(getRequest);
    // ...
}
```

This defeats the purpose of a batch API.

**Solution:**
- Build single SQL query with IN clause or UNION ALL
- Execute once and map results
- 10-100x performance improvement for large batches

**Estimated Effort:** 4-6 hours

---

### 10. Consistent Reads Not Implemented
**Priority:** MEDIUM  
**File:** `PdbItemManager.java` (getItem, batchGetItem, query)

**Problem:**  
DynamoDB supports `ConsistentRead` parameter for strongly consistent reads. Pretender ignores this parameter.

**Impact:**
- For SQL backends (PostgreSQL), this probably doesn't matter since reads are consistent by default
- But diverges from API contract
- Documentation should clarify this

**Solution:**
- Document that all reads are strongly consistent (SQL behavior)
- Optionally log warning if `ConsistentRead=false` requested
- Or implement eventual consistency simulation (complex, probably unnecessary)

**Estimated Effort:** 1 hour (documentation), 20+ hours (implementation)

---

### 11. ReturnConsumedCapacity Not Tracked
**Priority:** MEDIUM  
**File:** All operation methods

**Problem:**  
DynamoDB returns consumed capacity units when `ReturnConsumedCapacity` is requested. Pretender doesn't calculate or return this.

**Impact:**
- Cannot track/estimate costs
- Monitoring and alerting won't work

**Solution:**
- Implement capacity calculation (item size / 4KB for reads, item size / 1KB for writes)
- Return in response `ConsumedCapacity` field
- Could be complex to calculate accurately for queries/scans

**Estimated Effort:** 6-8 hours

---

### 12. TransactGetItems Projection May Not Work
**Priority:** MEDIUM  
**File:** `PdbItemManager.java` (transactGetItems, line 864)

**Problem:**  
TransactGetItems passes `projectionExpression` to getItem, but need to verify this works correctly for all cases.

**Solution:**
- Add integration test for transactGetItems with projection
- Verify only requested attributes are returned

**Estimated Effort:** 1 hour

---

### 13. Missing NULL and Empty String Validation
**Priority:** MEDIUM  
**File:** `PdbItemManager.java` (putItem, updateItem)

**Problem:**  
DynamoDB has special rules:
- Empty strings are not allowed in key attributes
- NULL attribute values must use NULL type, not absent
- Binary attributes cannot be empty

Pretender may not validate these correctly.

**Solution:**
- Add validation in `ItemConverter` or `PdbItemManager`
- Throw `ValidationException` for invalid values

**Estimated Effort:** 2-3 hours

---

### 14. Streams: Shard Management Not Implemented
**Priority:** MEDIUM  
**File:** `PdbStreamManager.java`

**Problem:**  
DynamoDB Streams has shards that split/merge over time. Pretender likely uses a single static shard per stream.

**Impact:**
- Works fine for small/medium loads
- Won't scale like real DynamoDB Streams
- Shard iterator logic may be oversimplified

**Solution:**
- Review shard implementation
- Document limitations vs real DynamoDB Streams
- Consider implementing basic shard splitting for very large streams

**Estimated Effort:** 8-12 hours

---

## 🟢 Low Priority / Enhancements

### 15. Code Duplication in DAO Mapping
**Priority:** LOW  
**File:** `PdbItemDao.java` (query, scan methods)

**Problem:**  
ResultSet to PdbItem mapping code is duplicated across multiple methods:
```java
.map((rs, ctx) -> {
    final PdbItem item = ImmutablePdbItem.builder()
        .tableName(tableName)
        .hashKeyValue(rs.getString("hash_key_value"))
        // ... repeated in 3+ places
```

**Solution:**
- Extract to private method or RowMapper class
- Reduces duplication and makes changes easier

**Estimated Effort:** 1-2 hours

---

### 16. Improved Error Messages
**Priority:** LOW  
**Files:** Various

**Problem:**  
Some error messages could be more helpful:
- "Table not found: X" could include list of available tables
- "Condition check failed" could show what condition was evaluated
- Expression parsing errors could show position in expression

**Solution:**
- Enhance exception messages throughout
- Add more context to errors

**Estimated Effort:** 2-4 hours

---

### 17. Comprehensive JavaDoc Documentation
**Priority:** LOW  
**Files:** Various

**Problem:**  
Some methods lack detailed JavaDoc, especially:
- Edge case behavior
- Exception conditions
- Parameter constraints

**Solution:**
- Add @throws tags for all exceptions
- Document all parameters and return values
- Add @see tags for related methods

**Estimated Effort:** 4-6 hours

---

### 18. Parallel Scan Not Supported
**Priority:** LOW  
**File:** `PdbItemManager.java`

**Problem:**  
DynamoDB supports parallel scan using `Segment` and `TotalSegments` parameters. Pretender doesn't implement this.

**Impact:**
- Cannot parallelize large table scans
- Performance gap vs real DynamoDB

**Solution:**
- Add support for Segment/TotalSegments
- Use MODULO on hash key for segmentation
- `WHERE MOD(hash_code(hash_key_value), :total) = :segment`

**Estimated Effort:** 4-6 hours

---

### 19. Local Secondary Indexes (LSI)
**Priority:** LOW  
**File:** New feature

**Problem:**  
LSIs are mentioned in IMPLEMENTATION_SUMMARY.md as a future enhancement but not implemented.

**Solution:**
- Similar to GSI implementation
- LSI shares partition key with main table
- Simpler than GSI in some ways

**Estimated Effort:** 12-16 hours

---

### 20. PartiQL Support
**Priority:** LOW  
**File:** New feature

**Problem:**  
DynamoDB now supports PartiQL (SQL-like query language). Pretender doesn't.

**Solution:**
- Add new `executeStatement()` method
- Parse PartiQL to internal query representation
- Could be complex to implement fully

**Estimated Effort:** 40+ hours

---

### 21. Attribute-Level Encryption
**Priority:** LOW  
**File:** New feature

**Problem:**  
DynamoDB supports client-side encryption of attributes. Pretender doesn't.

**Solution:**
- Add encryption/decryption layer in AttributeValueConverter
- Support AWS Encryption SDK format
- Optional feature, could be useful for compliance

**Estimated Effort:** 16-24 hours

---

### 22. Performance Metrics and Monitoring
**Priority:** LOW  
**File:** All managers

**Problem:**  
No built-in metrics for:
- Operation latency
- Item counts
- Storage size
- Query patterns

**Solution:**
- Add Micrometer metrics
- Track operation counts and latencies
- Expose via JMX or Prometheus

**Estimated Effort:** 8-12 hours

---

### 23. Integration Tests for Edge Cases
**Priority:** LOW  
**File:** Test files

**Missing test coverage:**
- Concurrent updates to same item (optimistic locking)
- Transaction with 25 items (max limit)
- Batch operations with max items
- Items near 400KB size limit
- Complex nested UpdateExpression (multiple list operations)
- GSI with ALL projection type and item updates
- Stream with 24-hour retention boundary
- TTL expiration during scan/query

**Solution:**
- Add targeted integration tests for each edge case

**Estimated Effort:** 8-12 hours

---

### 24. Database Connection Pool Configuration
**Priority:** LOW  
**File:** Configuration, JDBI setup

**Problem:**  
Connection pool settings may not be optimized for production use.

**Solution:**
- Review HikariCP configuration
- Add configurable pool size, timeout, etc.
- Document recommended settings

**Estimated Effort:** 2-4 hours

---

### 25. SQL Query Performance Analysis
**Priority:** LOW  
**File:** All DAOs

**Problem:**  
May be missing indexes or using inefficient queries.

**Solution:**
- Run EXPLAIN on all queries
- Identify missing indexes
- Optimize query patterns
- Add composite indexes where beneficial

**Estimated Effort:** 4-8 hours

---

## Summary

**Total identified items:** 25

**By Priority:**
- 🔴 Critical: 2
- 🟠 High: 7
- 🟡 Medium: 8
- 🟢 Low: 8

**Recommended Next Steps:**

1. **Immediate (Critical):**
   - Fix transaction atomicity using `jdbi.inTransaction()`
   - Fix batch write error handling and unprocessed items

2. **Short-term (High Priority):**
   - Implement scan pagination with ExclusiveStartKey
   - Add validation for transaction/batch limits
   - Implement item size validation

3. **Medium-term (Medium Priority):**
   - Optimize GSI maintenance and batch operations for performance
   - Add better documentation for consistent reads behavior
   - Enhance error messages

4. **Long-term (Low Priority):**
   - Consider LSI support
   - Add comprehensive metrics
   - Explore PartiQL support if needed

---

**Estimated Total Effort:**
- Critical + High: 16-24 hours
- Medium: 32-48 hours
- Low: 100+ hours

**Note:** These estimates assume familiarity with the codebase and may vary based on testing requirements and code review time.
