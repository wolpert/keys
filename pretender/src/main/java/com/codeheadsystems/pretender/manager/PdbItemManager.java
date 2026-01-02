package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.pretender.converter.AttributeValueConverter;
import com.codeheadsystems.pretender.converter.ItemConverter;
import com.codeheadsystems.pretender.dao.PdbItemDao;
import com.codeheadsystems.pretender.expression.ConditionExpressionParser;
import com.codeheadsystems.pretender.expression.KeyConditionExpressionParser;
import com.codeheadsystems.pretender.expression.UpdateExpressionParser;
import com.codeheadsystems.pretender.model.PdbGlobalSecondaryIndex;
import com.codeheadsystems.pretender.model.PdbItem;
import com.codeheadsystems.pretender.model.PdbMetadata;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Manager for DynamoDB item operations.
 * Implements putItem, getItem, updateItem, deleteItem, query, and scan.
 */
@Singleton
public class PdbItemManager {

  private static final Logger log = LoggerFactory.getLogger(PdbItemManager.class);

  private final PdbTableManager tableManager;
  private final PdbItemTableManager itemTableManager;
  private final PdbItemDao itemDao;
  private final ItemConverter itemConverter;
  private final AttributeValueConverter attributeValueConverter;
  private final ConditionExpressionParser conditionExpressionParser;
  private final KeyConditionExpressionParser keyConditionExpressionParser;
  private final UpdateExpressionParser updateExpressionParser;
  private final GsiProjectionHelper gsiProjectionHelper;
  private final com.codeheadsystems.pretender.helper.StreamCaptureHelper streamCaptureHelper;
  private final Clock clock;
  private final org.jdbi.v3.core.Jdbi jdbi;

  /**
   * Instantiates a new Pdb item manager.
   *
   * @param tableManager                 the table manager
   * @param itemTableManager             the item table manager
   * @param itemDao                      the item dao
   * @param itemConverter                the item converter
   * @param attributeValueConverter      the attribute value converter
   * @param conditionExpressionParser    the condition expression parser
   * @param keyConditionExpressionParser the key condition expression parser
   * @param updateExpressionParser       the update expression parser
   * @param gsiProjectionHelper          the GSI projection helper
   * @param streamCaptureHelper          the stream capture helper
   * @param clock                        the clock
   * @param jdbi                         the jdbi instance
   */
  @Inject
  public PdbItemManager(final PdbTableManager tableManager,
                        final PdbItemTableManager itemTableManager,
                        final PdbItemDao itemDao,
                        final ItemConverter itemConverter,
                        final AttributeValueConverter attributeValueConverter,
                        final ConditionExpressionParser conditionExpressionParser,
                        final KeyConditionExpressionParser keyConditionExpressionParser,
                        final UpdateExpressionParser updateExpressionParser,
                        final GsiProjectionHelper gsiProjectionHelper,
                        final com.codeheadsystems.pretender.helper.StreamCaptureHelper streamCaptureHelper,
                        final Clock clock,
                        final org.jdbi.v3.core.Jdbi jdbi) {
    log.info("PdbItemManager({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        tableManager, itemTableManager, itemDao, itemConverter, attributeValueConverter,
        conditionExpressionParser, keyConditionExpressionParser, updateExpressionParser, gsiProjectionHelper,
        streamCaptureHelper, clock, jdbi);
    this.tableManager = tableManager;
    this.itemTableManager = itemTableManager;
    this.itemDao = itemDao;
    this.itemConverter = itemConverter;
    this.attributeValueConverter = attributeValueConverter;
    this.conditionExpressionParser = conditionExpressionParser;
    this.keyConditionExpressionParser = keyConditionExpressionParser;
    this.updateExpressionParser = updateExpressionParser;
    this.gsiProjectionHelper = gsiProjectionHelper;
    this.streamCaptureHelper = streamCaptureHelper;
    this.clock = clock;
    this.jdbi = jdbi;
  }

  /**
   * Put item.
   *
   * @param request the request
   * @return the response
   * @throws IllegalArgumentException if key attributes contain empty strings, binary attributes are empty, or item size exceeds 400KB
   */
  public PutItemResponse putItem(final PutItemRequest request) {
    log.trace("putItem({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Validate item attributes (empty strings in keys, empty binary, etc.)
    validateItemAttributes(request.item(), metadata);

    // Validate item size (400KB limit)
    validateItemSize(request.item());

    // Extract key values
    final String hashKeyValue = attributeValueConverter.extractKeyValue(request.item(), metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(request.item(), sk));

    // Check if item exists (needed for both condition check and upsert logic)
    final Optional<PdbItem> existingPdbItem = itemDao.get(itemTableName(tableName), hashKeyValue, sortKeyValue);

    // Check condition expression if provided
    if (request.conditionExpression() != null && !request.conditionExpression().isBlank()) {
      // Convert existing item to AttributeValue map (null if doesn't exist)
      final Map<String, AttributeValue> existingItem = existingPdbItem
          .map(item -> attributeValueConverter.fromJson(item.attributesJson()))
          .orElse(null);

      // Evaluate condition
      final boolean conditionMet = conditionExpressionParser.evaluate(
          existingItem,
          request.conditionExpression(),
          request.expressionAttributeValues(),
          request.expressionAttributeNames()
      );

      if (!conditionMet) {
        throw ConditionalCheckFailedException.builder()
            .message("The conditional request failed")
            .build();
      }
    }

    // Capture stream event BEFORE actual write
    if (existingPdbItem.isPresent()) {
      // MODIFY event - item exists
      final Map<String, AttributeValue> oldItem = attributeValueConverter.fromJson(
          existingPdbItem.get().attributesJson());
      streamCaptureHelper.captureModify(tableName, oldItem, request.item());
    } else {
      // INSERT event - new item
      streamCaptureHelper.captureInsert(tableName, request.item());
    }

    // Convert to PdbItem
    final PdbItem pdbItem = itemConverter.toPdbItem(tableName, request.item(), metadata);

    // Insert or update (putItem is an upsert operation)
    if (existingPdbItem.isPresent()) {
      itemDao.update(itemTableName(tableName), pdbItem);
    } else {
      itemDao.insert(itemTableName(tableName), pdbItem);
    }

    // Maintain GSI tables
    maintainGsiTables(metadata, request.item(), pdbItem);

    // Build response
    return PutItemResponse.builder().build();
  }

  /**
   * Get item.
   *
   * @param request the request
   * @return the response
   */
  public GetItemResponse getItem(final GetItemRequest request) {
    log.trace("getItem({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract keys
    final String hashKeyValue = attributeValueConverter.extractKeyValue(
        request.key(), metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(request.key(), sk));

    // Query
    final Optional<PdbItem> pdbItem = itemDao.get(
        itemTableName(tableName), hashKeyValue, sortKeyValue);

    if (pdbItem.isEmpty()) {
      return GetItemResponse.builder().build();
    }

    // Convert to AttributeValue map
    Map<String, AttributeValue> item = attributeValueConverter.fromJson(
        pdbItem.get().attributesJson());

    // Check TTL expiration and delete if expired
    if (isExpired(metadata, item)) {
      log.debug("Item expired due to TTL, deleting and returning empty response");
      // Delete the expired item (on-read cleanup)
      itemDao.delete(itemTableName(tableName), hashKeyValue, sortKeyValue);
      // Also delete from GSI tables
      deleteFromGsiTables(metadata, item);
      return GetItemResponse.builder().build();
    }

    // Apply projection if present
    if (request.projectionExpression() != null && !request.projectionExpression().isBlank()) {
      item = itemConverter.applyProjection(item, request.projectionExpression());
    }

    return GetItemResponse.builder()
        .item(item)
        .build();
  }

  /**
   * Update item.
   *
   * @param request the request
   * @return the response
   * @throws IllegalArgumentException if updated attributes contain empty strings in keys, empty binary attributes, or item size exceeds 400KB
   */
  public UpdateItemResponse updateItem(final UpdateItemRequest request) {
    log.trace("updateItem({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract keys
    final String hashKeyValue = attributeValueConverter.extractKeyValue(
        request.key(), metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(request.key(), sk));

    // Get existing item or create empty one
    final Optional<PdbItem> existingPdbItem = itemDao.get(
        itemTableName(tableName), hashKeyValue, sortKeyValue);

    Map<String, AttributeValue> currentAttributes;
    if (existingPdbItem.isPresent()) {
      currentAttributes = new HashMap<>(attributeValueConverter.fromJson(
          existingPdbItem.get().attributesJson()));
    } else {
      currentAttributes = new HashMap<>(request.key());
    }

    // Apply update expression
    final Map<String, AttributeValue> updatedAttributes = updateExpressionParser.applyUpdate(
        currentAttributes,
        request.updateExpression(),
        request.expressionAttributeValues(),
        request.expressionAttributeNames());

    // Validate updated attributes (empty strings in keys, empty binary, etc.)
    validateItemAttributes(updatedAttributes, metadata);

    // Validate item size (400KB limit)
    validateItemSize(updatedAttributes);

    // Capture stream event BEFORE actual write
    if (existingPdbItem.isPresent()) {
      // MODIFY event - item exists
      streamCaptureHelper.captureModify(tableName, currentAttributes, updatedAttributes);
    } else {
      // INSERT event - item created via updateItem
      streamCaptureHelper.captureInsert(tableName, updatedAttributes);
    }

    // Convert to PdbItem and save
    final PdbItem updatedPdbItem;
    if (existingPdbItem.isPresent()) {
      updatedPdbItem = itemConverter.updatePdbItem(
          existingPdbItem.get(), updatedAttributes, metadata);
    } else {
      updatedPdbItem = itemConverter.toPdbItem(tableName, updatedAttributes, metadata);
    }

    if (existingPdbItem.isPresent()) {
      itemDao.update(itemTableName(tableName), updatedPdbItem);
    } else {
      itemDao.insert(itemTableName(tableName), updatedPdbItem);
    }

    // Maintain GSI tables
    maintainGsiTables(metadata, updatedAttributes, updatedPdbItem);

    // Build response
    final UpdateItemResponse.Builder responseBuilder = UpdateItemResponse.builder();

    if (request.returnValues() == ReturnValue.ALL_NEW) {
      responseBuilder.attributes(updatedAttributes);
    } else if (request.returnValues() == ReturnValue.ALL_OLD && existingPdbItem.isPresent()) {
      responseBuilder.attributes(currentAttributes);
    }

    return responseBuilder.build();
  }

  /**
   * Delete item.
   *
   * @param request the request
   * @return the response
   */
  public DeleteItemResponse deleteItem(final DeleteItemRequest request) {
    log.trace("deleteItem({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract keys
    final String hashKeyValue = attributeValueConverter.extractKeyValue(
        request.key(), metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(request.key(), sk));

    // Get old item (needed for return values, condition check, and GSI deletion)
    Map<String, AttributeValue> oldItem = null;
    final Optional<PdbItem> pdbItem = itemDao.get(
        itemTableName(tableName), hashKeyValue, sortKeyValue);
    if (pdbItem.isPresent()) {
      oldItem = attributeValueConverter.fromJson(pdbItem.get().attributesJson());
    }

    // Check condition expression if provided
    if (request.conditionExpression() != null && !request.conditionExpression().isBlank()) {
      // Evaluate condition against existing item (null if doesn't exist)
      final boolean conditionMet = conditionExpressionParser.evaluate(
          oldItem,
          request.conditionExpression(),
          request.expressionAttributeValues(),
          request.expressionAttributeNames()
      );

      if (!conditionMet) {
        throw ConditionalCheckFailedException.builder()
            .message("The conditional request failed")
            .build();
      }
    }

    // Capture REMOVE stream event BEFORE actual delete
    if (oldItem != null) {
      streamCaptureHelper.captureRemove(tableName, oldItem);
    }

    // Delete from GSI tables first if item exists
    if (oldItem != null) {
      deleteFromGsiTables(metadata, oldItem);
    }

    // Delete from main table
    itemDao.delete(itemTableName(tableName), hashKeyValue, sortKeyValue);

    // Build response
    final DeleteItemResponse.Builder responseBuilder = DeleteItemResponse.builder();
    if (oldItem != null) {
      responseBuilder.attributes(oldItem);
    }

    return responseBuilder.build();
  }

  /**
   * Query items.
   *
   * @param request the request
   * @return the response
   */
  public QueryResponse query(final QueryRequest request) {
    log.trace("query({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Determine if querying an index or the main table
    final String queryTableName;
    final String hashKeyName;
    final Optional<String> sortKeyName;

    if (request.indexName() != null && !request.indexName().isBlank()) {
      // Query GSI
      final PdbGlobalSecondaryIndex gsi = metadata.globalSecondaryIndexes().stream()
          .filter(g -> g.indexName().equals(request.indexName()))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "Index not found: " + request.indexName()));

      queryTableName = itemTableManager.getGsiTableName(tableName, gsi.indexName());
      hashKeyName = gsi.hashKey();
      sortKeyName = gsi.sortKey();
    } else {
      // Query main table
      queryTableName = itemTableName(tableName);
      hashKeyName = metadata.hashKey();
      sortKeyName = metadata.sortKey();
    }

    // Parse key condition expression
    final KeyConditionExpressionParser.ParsedKeyCondition condition =
        keyConditionExpressionParser.parse(
            request.keyConditionExpression(),
            request.expressionAttributeValues(),
            request.expressionAttributeNames());

    // Determine limit (default to 100 if not specified)
    final int limit = request.limit() != null ? request.limit() : 100;

    // Extract ExclusiveStartKey for pagination
    final Optional<String> exclusiveStartHashKey;
    final Optional<String> exclusiveStartSortKey;
    if (request.hasExclusiveStartKey() && request.exclusiveStartKey() != null) {
      exclusiveStartHashKey = Optional.ofNullable(request.exclusiveStartKey().get(hashKeyName))
          .map(av -> attributeValueConverter.extractKeyValue(request.exclusiveStartKey(), hashKeyName));
      exclusiveStartSortKey = sortKeyName.isPresent()
          ? Optional.ofNullable(request.exclusiveStartKey().get(sortKeyName.get()))
              .map(av -> attributeValueConverter.extractKeyValue(request.exclusiveStartKey(), sortKeyName.get()))
          : Optional.empty();
    } else {
      exclusiveStartHashKey = Optional.empty();
      exclusiveStartSortKey = Optional.empty();
    }

    // Query with limit + 1 to detect if there are more results
    final List<PdbItem> items = itemDao.query(
        queryTableName,
        condition.hashKeyValue(),
        condition.sortKeyCondition(),
        condition.sortKeyValue(),
        limit + 1,
        exclusiveStartHashKey,
        exclusiveStartSortKey);

    // Check if we have more results
    final boolean hasMore = items.size() > limit;
    final List<PdbItem> resultItems = hasMore ? items.subList(0, limit) : items;

    // Convert to AttributeValue maps and filter expired items
    final List<Map<String, AttributeValue>> resultAttributeMaps = new ArrayList<>();
    for (PdbItem item : resultItems) {
      Map<String, AttributeValue> attributes = attributeValueConverter.fromJson(
          item.attributesJson());

      // Check TTL expiration
      if (isExpired(metadata, attributes)) {
        log.debug("Skipping expired item in query results");
        continue;
      }

      // Apply FilterExpression if present (post-query filtering)
      if (request.filterExpression() != null && !request.filterExpression().isBlank()) {
        final boolean filterPassed = conditionExpressionParser.evaluate(
            attributes,
            request.filterExpression(),
            request.expressionAttributeValues(),
            request.expressionAttributeNames());
        if (!filterPassed) {
          log.trace("Item filtered out by FilterExpression");
          continue;
        }
      }

      // Apply projection if present
      if (request.projectionExpression() != null && !request.projectionExpression().isBlank()) {
        attributes = itemConverter.applyProjection(attributes, request.projectionExpression());
      }

      resultAttributeMaps.add(attributes);
    }

    // Build response
    final QueryResponse.Builder responseBuilder = QueryResponse.builder()
        .items(resultAttributeMaps)
        .count(resultAttributeMaps.size());

    // Add LastEvaluatedKey if there are more results
    if (hasMore) {
      final PdbItem lastItem = resultItems.get(resultItems.size() - 1);
      final Map<String, AttributeValue> lastKey = new HashMap<>();
      lastKey.put(hashKeyName,
          AttributeValue.builder().s(lastItem.hashKeyValue()).build());
      lastItem.sortKeyValue().ifPresent(sk ->
          lastKey.put(sortKeyName.orElseThrow(),
              AttributeValue.builder().s(sk).build()));
      responseBuilder.lastEvaluatedKey(lastKey);
    }

    return responseBuilder.build();
  }

  /**
   * Scan items.
   *
   * @param request the request
   * @return the response
   */
  public ScanResponse scan(final ScanRequest request) {
    log.trace("scan({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Determine limit (default to 100 if not specified)
    final int limit = request.limit() != null ? request.limit() : 100;

    // Extract ExclusiveStartKey for pagination
    Optional<String> exclusiveStartHashKey = Optional.empty();
    Optional<String> exclusiveStartSortKey = Optional.empty();

    if (request.hasExclusiveStartKey() && request.exclusiveStartKey() != null) {
      exclusiveStartHashKey = Optional.ofNullable(request.exclusiveStartKey().get(metadata.hashKey()))
          .map(av -> attributeValueConverter.extractKeyValue(request.exclusiveStartKey(), metadata.hashKey()));
      exclusiveStartSortKey = metadata.sortKey().isPresent()
          ? Optional.ofNullable(request.exclusiveStartKey().get(metadata.sortKey().get()))
              .map(av -> attributeValueConverter.extractKeyValue(request.exclusiveStartKey(), metadata.sortKey().get()))
          : Optional.empty();
    }

    // Scan with limit + 1 to detect if there are more results
    final List<PdbItem> items = itemDao.scan(itemTableName(tableName), limit + 1,
        exclusiveStartHashKey, exclusiveStartSortKey);

    // Check if we have more results
    final boolean hasMore = items.size() > limit;
    final List<PdbItem> resultItems = hasMore ? items.subList(0, limit) : items;

    // Convert to AttributeValue maps and filter expired items
    final List<Map<String, AttributeValue>> resultAttributeMaps = new ArrayList<>();
    for (PdbItem item : resultItems) {
      Map<String, AttributeValue> attributes = attributeValueConverter.fromJson(
          item.attributesJson());

      // Check TTL expiration
      if (isExpired(metadata, attributes)) {
        log.debug("Skipping expired item in scan results");
        continue;
      }

      // Apply FilterExpression if present (post-scan filtering)
      if (request.filterExpression() != null && !request.filterExpression().isBlank()) {
        final boolean filterPassed = conditionExpressionParser.evaluate(
            attributes,
            request.filterExpression(),
            request.expressionAttributeValues(),
            request.expressionAttributeNames());
        if (!filterPassed) {
          log.trace("Item filtered out by FilterExpression");
          continue;
        }
      }

      // Apply projection if present
      if (request.projectionExpression() != null && !request.projectionExpression().isBlank()) {
        attributes = itemConverter.applyProjection(attributes, request.projectionExpression());
      }

      resultAttributeMaps.add(attributes);
    }

    // Build response
    final ScanResponse.Builder responseBuilder = ScanResponse.builder()
        .items(resultAttributeMaps)
        .count(resultAttributeMaps.size());

    // Add LastEvaluatedKey if there are more results
    if (hasMore) {
      final PdbItem lastItem = resultItems.get(resultItems.size() - 1);
      final Map<String, AttributeValue> lastKey = new HashMap<>();
      lastKey.put(metadata.hashKey(),
          AttributeValue.builder().s(lastItem.hashKeyValue()).build());
      lastItem.sortKeyValue().ifPresent(sk ->
          lastKey.put(metadata.sortKey().orElseThrow(),
              AttributeValue.builder().s(sk).build()));
      responseBuilder.lastEvaluatedKey(lastKey);
    }

    return responseBuilder.build();
  }

  /**
   * Get table metadata, throwing ResourceNotFoundException if not found.
   */
  private PdbMetadata getTableMetadata(final String tableName) {
    return tableManager.getPdbTable(tableName)
        .orElseThrow(() -> ResourceNotFoundException.builder()
            .message("Table not found: " + tableName)
            .build());
  }

  /**
   * Get the item table name for a DynamoDB table.
   */
  private String itemTableName(final String tableName) {
    return "pdb_item_" + tableName.toLowerCase();
  }

  /**
   * Maintains GSI tables when an item is put or updated.
   *
   * @param metadata  the table metadata
   * @param itemAttrs the item attributes
   * @param pdbItem   the PdbItem for create/update timestamps
   */
  private void maintainGsiTables(final PdbMetadata metadata,
                                 final Map<String, AttributeValue> itemAttrs,
                                 final PdbItem pdbItem) {
    if (metadata.globalSecondaryIndexes().isEmpty()) {
      return;
    }

    for (PdbGlobalSecondaryIndex gsi : metadata.globalSecondaryIndexes()) {
      // Check if item has GSI keys
      if (!gsiProjectionHelper.hasGsiKeys(itemAttrs, gsi)) {
        log.debug("Item missing GSI keys for {}, skipping GSI maintenance", gsi.indexName());
        continue;
      }

      // Extract GSI key values
      final String gsiHashKeyValue = attributeValueConverter.extractKeyValue(itemAttrs, gsi.hashKey());
      final Optional<String> gsiSortKeyValue = gsi.sortKey().map(sk ->
          attributeValueConverter.extractKeyValue(itemAttrs, sk));

      // Build composite sort key for uniqueness
      // Format: [<gsi_sort_key>#]<main_hash_key>[#<main_sort_key>]
      final String compositeSortKey = buildCompositeSortKey(
          gsiSortKeyValue,
          pdbItem.hashKeyValue(),
          pdbItem.sortKeyValue()
      );

      // Apply projection
      final Map<String, AttributeValue> projectedAttrs = gsiProjectionHelper.applyProjection(
          itemAttrs, gsi, metadata.hashKey(), metadata.sortKey().orElse(null));

      // Convert to JSON
      final String projectedJson = attributeValueConverter.toJson(projectedAttrs);

      // Create PdbItem for GSI table
      final PdbItem gsiItem = com.codeheadsystems.pretender.model.ImmutablePdbItem.builder()
          .tableName(itemTableManager.getGsiTableName(metadata.name(), gsi.indexName()))
          .hashKeyValue(gsiHashKeyValue)
          .sortKeyValue(compositeSortKey)  // Always non-null for GSI tables
          .attributesJson(projectedJson)
          .createDate(pdbItem.createDate())
          .updateDate(pdbItem.updateDate())
          .build();

      // Insert or replace in GSI table
      final String gsiTableName = itemTableManager.getGsiTableName(metadata.name(), gsi.indexName());
      itemDao.insert(gsiTableName, gsiItem);

      log.trace("Maintained GSI table {} for item", gsiTableName);
    }
  }

  /**
   * Deletes an item from all GSI tables.
   *
   * @param metadata  the table metadata
   * @param itemAttrs the item attributes (to extract GSI keys)
   */
  private void deleteFromGsiTables(final PdbMetadata metadata,
                                   final Map<String, AttributeValue> itemAttrs) {
    if (metadata.globalSecondaryIndexes().isEmpty()) {
      return;
    }

    // Extract main table keys once
    final String mainHashKeyValue = attributeValueConverter.extractKeyValue(itemAttrs, metadata.hashKey());
    final Optional<String> mainSortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(itemAttrs, sk));

    for (PdbGlobalSecondaryIndex gsi : metadata.globalSecondaryIndexes()) {
      // Check if item has GSI keys
      if (!gsiProjectionHelper.hasGsiKeys(itemAttrs, gsi)) {
        continue;
      }

      // Extract GSI key values
      final String gsiHashKeyValue = attributeValueConverter.extractKeyValue(itemAttrs, gsi.hashKey());
      final Optional<String> gsiSortKeyValue = gsi.sortKey().map(sk ->
          attributeValueConverter.extractKeyValue(itemAttrs, sk));

      // Build composite sort key for uniqueness (same as in maintainGsiTables)
      final String compositeSortKey = buildCompositeSortKey(
          gsiSortKeyValue,
          mainHashKeyValue,
          mainSortKeyValue
      );

      // Delete from GSI table
      final String gsiTableName = itemTableManager.getGsiTableName(metadata.name(), gsi.indexName());
      itemDao.delete(gsiTableName, gsiHashKeyValue, Optional.of(compositeSortKey));

      log.trace("Deleted from GSI table {}", gsiTableName);
    }
  }

  /**
   * Checks if an item has expired based on TTL.
   *
   * @param metadata  the table metadata
   * @param itemAttrs the item attributes
   * @return true if the item is expired
   */
  private boolean isExpired(final PdbMetadata metadata, final Map<String, AttributeValue> itemAttrs) {
    if (!metadata.ttlEnabled() || metadata.ttlAttributeName().isEmpty()) {
      return false;
    }

    final String ttlAttrName = metadata.ttlAttributeName().get();
    if (!itemAttrs.containsKey(ttlAttrName)) {
      return false;
    }

    final AttributeValue ttlValue = itemAttrs.get(ttlAttrName);
    if (ttlValue.n() == null) {
      return false;
    }

    try {
      // TTL value is epoch seconds
      final long ttlEpochSeconds = Long.parseLong(ttlValue.n());
      final long currentEpochSeconds = clock.instant().getEpochSecond();
      return currentEpochSeconds > ttlEpochSeconds;
    } catch (NumberFormatException e) {
      log.warn("Invalid TTL value for attribute {}: {}", ttlAttrName, ttlValue.n());
      return false;
    }
  }

  /**
   * Builds a composite sort key for GSI tables to ensure uniqueness.
   * Format: [<gsi_sort_key>#]<main_hash_key>[#<main_sort_key>]
   *
   * @param gsiSortKeyValue  the GSI sort key value (if present)
   * @param mainHashKeyValue the main table hash key value
   * @param mainSortKeyValue the main table sort key value (if present)
   * @return the composite sort key
   */
  private String buildCompositeSortKey(final Optional<String> gsiSortKeyValue,
                                       final String mainHashKeyValue,
                                       final Optional<String> mainSortKeyValue) {
    final StringBuilder sb = new StringBuilder();

    // Add GSI sort key if present
    gsiSortKeyValue.ifPresent(gsk -> sb.append(gsk).append("#"));

    // Always add main hash key
    sb.append(mainHashKeyValue);

    // Add main sort key if present
    mainSortKeyValue.ifPresent(msk -> sb.append("#").append(msk));

    return sb.toString();
  }

  /**
   * Batch get items from one or more tables.
   *
   * @param request the batch get item request
   * @return the batch get item response
   * @throws IllegalArgumentException if the request contains more than 100 items
   */
  public BatchGetItemResponse batchGetItem(final BatchGetItemRequest request) {
    log.trace("batchGetItem({})", request);

    // Validate batch get item count (DynamoDB limit: 100 items max across all tables)
    if (request.requestItems() != null) {
      int totalItems = request.requestItems().values().stream()
          .mapToInt(keysAndAttributes -> keysAndAttributes.keys() != null ? keysAndAttributes.keys().size() : 0)
          .sum();
      if (totalItems > 100) {
        throw new IllegalArgumentException(
            "BatchGetItem request cannot contain more than 100 items (received " + totalItems + " items)");
      }
    }

    final Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();

    // Process each table in the request
    for (Map.Entry<String, KeysAndAttributes> entry : request.requestItems().entrySet()) {
      final String tableName = entry.getKey();
      final KeysAndAttributes keysAndAttributes = entry.getValue();
      final List<Map<String, AttributeValue>> items = new ArrayList<>();

      // Verify table exists
      final PdbMetadata metadata = getTableMetadata(tableName);

      // Get each item
      for (Map<String, AttributeValue> key : keysAndAttributes.keys()) {
        final GetItemRequest getRequest = GetItemRequest.builder()
            .tableName(tableName)
            .key(key)
            .projectionExpression(keysAndAttributes.projectionExpression())
            .expressionAttributeNames(keysAndAttributes.expressionAttributeNames())
            .build();

        final GetItemResponse getResponse = getItem(getRequest);
        if (getResponse.hasItem() && !getResponse.item().isEmpty()) {
          items.add(getResponse.item());
        }
      }

      if (!items.isEmpty()) {
        responses.put(tableName, items);
      }
    }

    return BatchGetItemResponse.builder()
        .responses(responses)
        .build();
  }

  /**
   * Batch write (put or delete) items to one or more tables.
   * Tracks failed items and returns them as unprocessed items.
   *
   * @param request the batch write item request
   * @return the batch write item response with unprocessed items if any failed
   * @throws IllegalArgumentException if the request contains more than 25 write requests
   */
  public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest request) {
    log.trace("batchWriteItem({})", request);

    // Validate batch write item count (DynamoDB limit: 25 requests max across all tables)
    if (request.requestItems() != null) {
      int totalRequests = request.requestItems().values().stream()
          .mapToInt(writeRequests -> writeRequests != null ? writeRequests.size() : 0)
          .sum();
      if (totalRequests > 25) {
        throw new IllegalArgumentException(
            "BatchWriteItem request cannot contain more than 25 requests (received " + totalRequests + " requests)");
      }
    }

    // Track unprocessed items (items that failed to write)
    final Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();

    // Process each table in the request
    for (Map.Entry<String, List<WriteRequest>> entry : request.requestItems().entrySet()) {
      final String tableName = entry.getKey();
      final List<WriteRequest> writeRequests = entry.getValue();

      // Verify table exists first - if table doesn't exist, all items for this table are unprocessed
      try {
        getTableMetadata(tableName);
      } catch (ResourceNotFoundException e) {
        // Table doesn't exist - all items for this table are unprocessed
        log.warn("Table {} not found, marking all {} items as unprocessed", tableName, writeRequests.size());
        unprocessedItems.put(tableName, new ArrayList<>(writeRequests));
        continue;  // Skip to next table
      }

      // Process each write request
      for (WriteRequest writeRequest : writeRequests) {
        try {
          if (writeRequest.putRequest() != null) {
            // Handle PutRequest
            final PutRequest putRequest = writeRequest.putRequest();
            putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(putRequest.item())
                .build());
          } else if (writeRequest.deleteRequest() != null) {
            // Handle DeleteRequest
            final DeleteRequest deleteRequest = writeRequest.deleteRequest();
            deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(deleteRequest.key())
                .build());
          }
        } catch (Exception e) {
          // Write failed - add to unprocessed items
          log.warn("Failed to write item to table {}: {}", tableName, e.getMessage());
          unprocessedItems.computeIfAbsent(tableName, k -> new ArrayList<>()).add(writeRequest);
        }
      }
    }

    // Return response with unprocessed items if any
    if (unprocessedItems.isEmpty()) {
      return BatchWriteItemResponse.builder().build();
    } else {
      log.info("BatchWriteItem completed with {} unprocessed item(s) across {} table(s)",
          unprocessedItems.values().stream().mapToInt(List::size).sum(),
          unprocessedItems.size());
      return BatchWriteItemResponse.builder()
          .unprocessedItems(unprocessedItems)
          .build();
    }
  }

  /**
   * Transactionally get items from one or more tables.
   * All gets must succeed or the entire transaction fails.
   *
   * @param request the transact get items request
   * @return the transact get items response with all retrieved items
   * @throws TransactionCanceledException if any get operation fails or table doesn't exist
   * @throws IllegalArgumentException if the request contains more than 25 items
   */
  public TransactGetItemsResponse transactGetItems(final TransactGetItemsRequest request) {
    log.trace("transactGetItems({})", request);

    // Validate transaction item count (DynamoDB limit: 25 items max)
    if (request.transactItems() != null && request.transactItems().size() > 25) {
      throw new IllegalArgumentException(
          "Transaction request cannot contain more than 25 items (received " +
              request.transactItems().size() + " items)");
    }

    final List<ItemResponse> responses = new ArrayList<>();

    try {
      // Process each get operation
      for (TransactGetItem transactGetItem : request.transactItems()) {
        final Get get = transactGetItem.get();
        final String tableName = get.tableName();

        // Verify table exists (will throw ResourceNotFoundException if not)
        getTableMetadata(tableName);

        // Execute the get operation
        final GetItemRequest getRequest = GetItemRequest.builder()
            .tableName(tableName)
            .key(get.key())
            .projectionExpression(get.projectionExpression())
            .expressionAttributeNames(get.expressionAttributeNames())
            .build();

        final GetItemResponse getResponse = getItem(getRequest);

        // Build item response
        final ItemResponse itemResponse = ItemResponse.builder()
            .item(getResponse.item())
            .build();

        responses.add(itemResponse);
      }

      return TransactGetItemsResponse.builder()
          .responses(responses)
          .build();

    } catch (ResourceNotFoundException e) {
      // Convert to TransactionCanceledException
      final CancellationReason reason = CancellationReason.builder()
          .code("ResourceNotFound")
          .message(e.getMessage())
          .build();

      throw TransactionCanceledException.builder()
          .message("Transaction cancelled")
          .cancellationReasons(reason)
          .build();
    } catch (Exception e) {
      // Handle any other exception
      final CancellationReason reason = CancellationReason.builder()
          .code("InternalError")
          .message(e.getMessage())
          .build();

      throw TransactionCanceledException.builder()
          .message("Transaction cancelled")
          .cancellationReasons(reason)
          .build();
    }
  }

  /**
   * Transactionally write items to one or more tables.
   * All writes must succeed (including condition checks) or the entire transaction fails.
   * Supports Put, Update, Delete, and ConditionCheck operations.
   *
   * @param request the transact write items request
   * @return the transact write items response (empty on success)
   * @throws TransactionCanceledException if any write operation fails or condition check fails
   * @throws IllegalArgumentException if the request contains more than 25 items
   */
  public TransactWriteItemsResponse transactWriteItems(final TransactWriteItemsRequest request) {
    log.trace("transactWriteItems({})", request);

    // Validate transaction item count (DynamoDB limit: 25 items max)
    if (request.transactItems() != null && request.transactItems().size() > 25) {
      throw new IllegalArgumentException(
          "Transaction request cannot contain more than 25 items (received " +
              request.transactItems().size() + " items)");
    }

    final List<CancellationReason> cancellationReasons = new ArrayList<>();

    try {
      // Wrap ALL operations in a single database transaction for true atomicity
      return jdbi.inTransaction(handle -> {
        // Process each write operation within the transaction
        int index = 0;
        for (TransactWriteItem transactWriteItem : request.transactItems()) {
          try {
            processTransactWriteItem(transactWriteItem, handle);
          } catch (ConditionalCheckFailedException e) {
            // Condition check failed - build cancellation reason
            final CancellationReason reason = CancellationReason.builder()
                .code("ConditionalCheckFailed")
                .message(e.getMessage())
                .build();
            cancellationReasons.add(reason);

            // Transaction failed - throw exception (will trigger rollback)
            throw TransactionCanceledException.builder()
                .message("Transaction cancelled due to conditional check failure")
                .cancellationReasons(cancellationReasons)
                .build();

          } catch (ResourceNotFoundException e) {
            // Table not found - build cancellation reason
            final CancellationReason reason = CancellationReason.builder()
                .code("ResourceNotFound")
                .message(e.getMessage())
                .build();
            cancellationReasons.add(reason);

            throw TransactionCanceledException.builder()
                .message("Transaction cancelled due to resource not found")
                .cancellationReasons(cancellationReasons)
                .build();
          }
          index++;
        }

        // All operations succeeded - commit transaction
        return TransactWriteItemsResponse.builder().build();
      });

    } catch (TransactionCanceledException e) {
      // Re-throw transaction cancelled exceptions
      throw e;
    } catch (Exception e) {
      // Handle any other unexpected exception
      final CancellationReason reason = CancellationReason.builder()
          .code("InternalError")
          .message(e.getMessage())
          .build();

      throw TransactionCanceledException.builder()
          .message("Transaction cancelled due to internal error")
          .cancellationReasons(reason)
          .build();
    }
  }

  /**
   * Processes a single transactional write item operation within a transaction.
   * NOTE: This method skips Stream and GSI updates for simplicity, as DynamoDB
   * streams are asynchronous and GSI updates are eventually consistent.
   *
   * @param transactWriteItem the transact write item
   * @param handle            the database handle for transactional operations
   * @throws ConditionalCheckFailedException if a condition check fails
   * @throws ResourceNotFoundException       if the table doesn't exist
   */
  private void processTransactWriteItem(final TransactWriteItem transactWriteItem, final Handle handle) {
    if (transactWriteItem.put() != null) {
      // Handle Put operation
      final Put put = transactWriteItem.put();
      processPutInTransaction(handle, put.tableName(), put.item(),
          put.conditionExpression(), put.expressionAttributeNames(), put.expressionAttributeValues());

    } else if (transactWriteItem.update() != null) {
      // Handle Update operation
      final Update update = transactWriteItem.update();
      processUpdateInTransaction(handle, update.tableName(), update.key(),
          update.updateExpression(), update.conditionExpression(),
          update.expressionAttributeNames(), update.expressionAttributeValues());

    } else if (transactWriteItem.delete() != null) {
      // Handle Delete operation
      final Delete delete = transactWriteItem.delete();
      processDeleteInTransaction(handle, delete.tableName(), delete.key(),
          delete.conditionExpression(), delete.expressionAttributeNames(), delete.expressionAttributeValues());

    } else if (transactWriteItem.conditionCheck() != null) {
      // Handle ConditionCheck operation
      final ConditionCheck conditionCheck = transactWriteItem.conditionCheck();
      processConditionCheckInTransaction(handle, conditionCheck.tableName(), conditionCheck.key(),
          conditionCheck.conditionExpression(), conditionCheck.expressionAttributeNames(),
          conditionCheck.expressionAttributeValues());
    }
  }

  /**
   * Process a Put operation within a transaction.
   */
  private void processPutInTransaction(final Handle handle, final String tableName,
                                       final Map<String, AttributeValue> item,
                                       final String conditionExpression,
                                       final Map<String, String> expressionAttributeNames,
                                       final Map<String, AttributeValue> expressionAttributeValues) {
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract key values
    final String hashKeyValue = attributeValueConverter.extractKeyValue(item, metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(item, sk));

    // Check if item exists (for condition evaluation)
    final String itemTableName = itemTableManager.getItemTableName(tableName);
    final Optional<PdbItem> existingPdbItem = itemDao.get(handle, itemTableName, hashKeyValue, sortKeyValue);

    // Check condition expression if provided
    if (conditionExpression != null && !conditionExpression.isBlank()) {
      final Map<String, AttributeValue> existingItem = existingPdbItem
          .map(i -> attributeValueConverter.fromJson(i.attributesJson()))
          .orElse(null);

      final boolean conditionMet = conditionExpressionParser.evaluate(
          existingItem, conditionExpression,
          expressionAttributeValues != null ? expressionAttributeValues : Collections.emptyMap(),
          expressionAttributeNames != null ? expressionAttributeNames : Collections.emptyMap()
      );

      if (!conditionMet) {
        throw ConditionalCheckFailedException.builder()
            .message("The conditional request failed")
            .build();
      }
    }

    // Convert to PdbItem
    final PdbItem pdbItem = itemConverter.toPdbItem(tableName, item, metadata);

    // Insert or update (putItem is an upsert operation)
    if (existingPdbItem.isPresent()) {
      itemDao.update(handle, itemTableName, pdbItem);
    } else {
      itemDao.insert(handle, itemTableName, pdbItem);
    }
  }

  /**
   * Process an Update operation within a transaction.
   */
  private void processUpdateInTransaction(final Handle handle, final String tableName,
                                          final Map<String, AttributeValue> key,
                                          final String updateExpression,
                                          final String conditionExpression,
                                          final Map<String, String> expressionAttributeNames,
                                          final Map<String, AttributeValue> expressionAttributeValues) {
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract key values
    final String hashKeyValue = attributeValueConverter.extractKeyValue(key, metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(key, sk));

    // Get existing item
    final String itemTableName = itemTableManager.getItemTableName(tableName);
    final Optional<PdbItem> existingPdbItem = itemDao.get(handle, itemTableName, hashKeyValue, sortKeyValue);

    // Get current attributes (or empty map if item doesn't exist - updateItem creates if not exists)
    Map<String, AttributeValue> currentAttributes = existingPdbItem
        .map(item -> attributeValueConverter.fromJson(item.attributesJson()))
        .orElse(new HashMap<>(key));  // Start with key if item doesn't exist

    // Check condition expression if provided
    if (conditionExpression != null && !conditionExpression.isBlank()) {
      final boolean conditionMet = conditionExpressionParser.evaluate(
          existingPdbItem.isPresent() ? currentAttributes : null,
          conditionExpression,
          expressionAttributeValues != null ? expressionAttributeValues : Collections.emptyMap(),
          expressionAttributeNames != null ? expressionAttributeNames : Collections.emptyMap()
      );

      if (!conditionMet) {
        throw ConditionalCheckFailedException.builder()
            .message("The conditional request failed")
            .build();
      }
    }

    // Apply update expression
    final Map<String, AttributeValue> updatedAttributes = updateExpressionParser.applyUpdate(
        currentAttributes, updateExpression,
        expressionAttributeValues != null ? expressionAttributeValues : Collections.emptyMap(),
        expressionAttributeNames != null ? expressionAttributeNames : Collections.emptyMap()
    );

    // Convert to PdbItem
    final PdbItem pdbItem = itemConverter.toPdbItem(tableName, updatedAttributes, metadata);

    // Insert or update
    if (existingPdbItem.isPresent()) {
      itemDao.update(handle, itemTableName, pdbItem);
    } else {
      itemDao.insert(handle, itemTableName, pdbItem);
    }
  }

  /**
   * Process a Delete operation within a transaction.
   */
  private void processDeleteInTransaction(final Handle handle, final String tableName,
                                          final Map<String, AttributeValue> key,
                                          final String conditionExpression,
                                          final Map<String, String> expressionAttributeNames,
                                          final Map<String, AttributeValue> expressionAttributeValues) {
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract key values
    final String hashKeyValue = attributeValueConverter.extractKeyValue(key, metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(key, sk));

    // Get existing item for condition check
    final String itemTableName = itemTableManager.getItemTableName(tableName);
    final Optional<PdbItem> existingPdbItem = itemDao.get(handle, itemTableName, hashKeyValue, sortKeyValue);

    // Check condition expression if provided
    if (conditionExpression != null && !conditionExpression.isBlank()) {
      final Map<String, AttributeValue> existingItem = existingPdbItem
          .map(item -> attributeValueConverter.fromJson(item.attributesJson()))
          .orElse(null);

      final boolean conditionMet = conditionExpressionParser.evaluate(
          existingItem, conditionExpression,
          expressionAttributeValues != null ? expressionAttributeValues : Collections.emptyMap(),
          expressionAttributeNames != null ? expressionAttributeNames : Collections.emptyMap()
      );

      if (!conditionMet) {
        throw ConditionalCheckFailedException.builder()
            .message("The conditional request failed")
            .build();
      }
    }

    // Delete the item
    itemDao.delete(handle, itemTableName, hashKeyValue, sortKeyValue);
  }

  /**
   * Process a ConditionCheck operation within a transaction.
   */
  private void processConditionCheckInTransaction(final Handle handle, final String tableName,
                                                   final Map<String, AttributeValue> key,
                                                   final String conditionExpression,
                                                   final Map<String, String> expressionAttributeNames,
                                                   final Map<String, AttributeValue> expressionAttributeValues) {
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Extract key values
    final String hashKeyValue = attributeValueConverter.extractKeyValue(key, metadata.hashKey());
    final Optional<String> sortKeyValue = metadata.sortKey().map(sk ->
        attributeValueConverter.extractKeyValue(key, sk));

    // Get the item to check condition against
    final String itemTableName = itemTableManager.getItemTableName(tableName);
    final Optional<PdbItem> existingItem = itemDao.get(handle, itemTableName, hashKeyValue, sortKeyValue);

    // If condition expression is present, evaluate it
    if (conditionExpression != null && !conditionExpression.isBlank()) {
      final Map<String, AttributeValue> currentItem = existingItem.isPresent()
          ? attributeValueConverter.fromJson(existingItem.get().attributesJson())
          : new HashMap<>();

      final boolean conditionMet = conditionExpressionParser.evaluate(
          currentItem, conditionExpression,
          expressionAttributeValues != null ? expressionAttributeValues : Collections.emptyMap(),
          expressionAttributeNames != null ? expressionAttributeNames : Collections.emptyMap()
      );

      if (!conditionMet) {
        throw ConditionalCheckFailedException.builder()
            .message("Condition check failed")
            .build();
      }
    }
  }

  /**
   * Validates item attributes according to DynamoDB rules.
   *
   * @param item the item to validate
   * @param metadata the table metadata (for key attribute names)
   * @throws IllegalArgumentException if validation fails
   */
  private void validateItemAttributes(final Map<String, AttributeValue> item, final PdbMetadata metadata) {
    // Validate hash key is not empty string
    final AttributeValue hashKeyAttr = item.get(metadata.hashKey());
    if (hashKeyAttr != null && hashKeyAttr.s() != null && hashKeyAttr.s().isEmpty()) {
      throw new IllegalArgumentException(
          "One or more parameter values were invalid: An AttributeValue may not contain an empty string. " +
          "Key: " + metadata.hashKey());
    }

    // Validate sort key is not empty string (if table has sort key)
    if (metadata.sortKey().isPresent()) {
      final AttributeValue sortKeyAttr = item.get(metadata.sortKey().get());
      if (sortKeyAttr != null && sortKeyAttr.s() != null && sortKeyAttr.s().isEmpty()) {
        throw new IllegalArgumentException(
            "One or more parameter values were invalid: An AttributeValue may not contain an empty string. " +
            "Key: " + metadata.sortKey().get());
      }
    }

    // Validate no binary attributes are empty
    for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
      final AttributeValue attrValue = entry.getValue();

      // Check binary attribute
      if (attrValue.b() != null && attrValue.b().asByteArray().length == 0) {
        throw new IllegalArgumentException(
            "One or more parameter values were invalid: Binary attributes cannot be empty. " +
            "Attribute: " + entry.getKey());
      }

      // Check binary set attribute
      if (attrValue.bs() != null) {
        for (software.amazon.awssdk.core.SdkBytes bytes : attrValue.bs()) {
          if (bytes.asByteArray().length == 0) {
            throw new IllegalArgumentException(
                "One or more parameter values were invalid: Binary set attributes cannot contain empty values. " +
                "Attribute: " + entry.getKey());
          }
        }
      }

      // Check string set - DynamoDB doesn't allow empty strings in string sets either
      if (attrValue.ss() != null) {
        for (String str : attrValue.ss()) {
          if (str.isEmpty()) {
            throw new IllegalArgumentException(
                "One or more parameter values were invalid: String set attributes cannot contain empty strings. " +
                "Attribute: " + entry.getKey());
          }
        }
      }
    }
  }

  /**
   * Validates item size according to DynamoDB rules (400KB max).
   *
   * @param item the item to validate
   * @throws IllegalArgumentException if item size exceeds 400KB
   */
  private void validateItemSize(final Map<String, AttributeValue> item) {
    // Convert item to JSON to calculate size
    final String json = attributeValueConverter.toJson(item);
    final int itemSizeBytes = json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

    // DynamoDB limit: 400KB (400,000 bytes)
    final int MAX_ITEM_SIZE_BYTES = 400_000;

    if (itemSizeBytes > MAX_ITEM_SIZE_BYTES) {
      throw new IllegalArgumentException(
          "Item size has exceeded the maximum allowed size of 400 KB " +
          "(actual size: " + itemSizeBytes + " bytes)");
    }
  }
}
