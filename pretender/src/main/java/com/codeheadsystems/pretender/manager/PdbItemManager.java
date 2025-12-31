package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.pretender.converter.AttributeValueConverter;
import com.codeheadsystems.pretender.converter.ItemConverter;
import com.codeheadsystems.pretender.dao.PdbItemDao;
import com.codeheadsystems.pretender.expression.KeyConditionExpressionParser;
import com.codeheadsystems.pretender.expression.UpdateExpressionParser;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
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

/**
 * Manager for DynamoDB item operations.
 * Implements putItem, getItem, updateItem, deleteItem, query, and scan.
 */
@Singleton
public class PdbItemManager {

  private static final Logger log = LoggerFactory.getLogger(PdbItemManager.class);

  private final PdbTableManager tableManager;
  private final PdbItemDao itemDao;
  private final ItemConverter itemConverter;
  private final AttributeValueConverter attributeValueConverter;
  private final KeyConditionExpressionParser keyConditionExpressionParser;
  private final UpdateExpressionParser updateExpressionParser;
  private final Clock clock;

  /**
   * Instantiates a new Pdb item manager.
   *
   * @param tableManager                   the table manager
   * @param itemDao                        the item dao
   * @param itemConverter                  the item converter
   * @param attributeValueConverter        the attribute value converter
   * @param keyConditionExpressionParser   the key condition expression parser
   * @param updateExpressionParser         the update expression parser
   * @param clock                          the clock
   */
  @Inject
  public PdbItemManager(final PdbTableManager tableManager,
                        final PdbItemDao itemDao,
                        final ItemConverter itemConverter,
                        final AttributeValueConverter attributeValueConverter,
                        final KeyConditionExpressionParser keyConditionExpressionParser,
                        final UpdateExpressionParser updateExpressionParser,
                        final Clock clock) {
    log.info("PdbItemManager({}, {}, {}, {}, {}, {}, {})",
        tableManager, itemDao, itemConverter, attributeValueConverter,
        keyConditionExpressionParser, updateExpressionParser, clock);
    this.tableManager = tableManager;
    this.itemDao = itemDao;
    this.itemConverter = itemConverter;
    this.attributeValueConverter = attributeValueConverter;
    this.keyConditionExpressionParser = keyConditionExpressionParser;
    this.updateExpressionParser = updateExpressionParser;
    this.clock = clock;
  }

  /**
   * Put item.
   *
   * @param request the request
   * @return the response
   */
  public PutItemResponse putItem(final PutItemRequest request) {
    log.trace("putItem({})", request);

    final String tableName = request.tableName();
    final PdbMetadata metadata = getTableMetadata(tableName);

    // Convert to PdbItem
    final PdbItem pdbItem = itemConverter.toPdbItem(tableName, request.item(), metadata);

    // Insert or replace
    itemDao.insert(itemTableName(tableName), pdbItem);

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

    // Get old item if needed for return values
    Map<String, AttributeValue> oldItem = null;
    if (request.returnValues() == ReturnValue.ALL_OLD) {
      final Optional<PdbItem> pdbItem = itemDao.get(
          itemTableName(tableName), hashKeyValue, sortKeyValue);
      if (pdbItem.isPresent()) {
        oldItem = attributeValueConverter.fromJson(pdbItem.get().attributesJson());
      }
    }

    // Delete
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

    // Parse key condition expression
    final KeyConditionExpressionParser.ParsedKeyCondition condition =
        keyConditionExpressionParser.parse(
            request.keyConditionExpression(),
            request.expressionAttributeValues());

    // Determine limit (default to 100 if not specified)
    final int limit = request.limit() != null ? request.limit() : 100;

    // Query with limit + 1 to detect if there are more results
    final List<PdbItem> items = itemDao.query(
        itemTableName(tableName),
        condition.hashKeyValue(),
        condition.sortKeyCondition(),
        condition.sortKeyValue(),
        limit + 1);

    // Check if we have more results
    final boolean hasMore = items.size() > limit;
    final List<PdbItem> resultItems = hasMore ? items.subList(0, limit) : items;

    // Convert to AttributeValue maps
    final List<Map<String, AttributeValue>> resultAttributeMaps = new ArrayList<>();
    for (PdbItem item : resultItems) {
      Map<String, AttributeValue> attributes = attributeValueConverter.fromJson(
          item.attributesJson());

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

    // Scan with limit + 1 to detect if there are more results
    // Note: ExclusiveStartKey is not supported in initial implementation
    final List<PdbItem> items = itemDao.scan(itemTableName(tableName), limit + 1);

    // Check if we have more results
    final boolean hasMore = items.size() > limit;
    final List<PdbItem> resultItems = hasMore ? items.subList(0, limit) : items;

    // Convert to AttributeValue maps
    final List<Map<String, AttributeValue>> resultAttributeMaps = new ArrayList<>();
    for (PdbItem item : resultItems) {
      Map<String, AttributeValue> attributes = attributeValueConverter.fromJson(
          item.attributesJson());

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
}
