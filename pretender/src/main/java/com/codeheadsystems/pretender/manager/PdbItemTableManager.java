package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.dbu.model.Database;
import com.codeheadsystems.pretender.model.PdbMetadata;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages dynamic creation and deletion of item storage tables.
 * Each DynamoDB table gets a corresponding SQL table for storing items.
 */
@Singleton
public class PdbItemTableManager {

  private static final Logger log = LoggerFactory.getLogger(PdbItemTableManager.class);
  private static final String TABLE_PREFIX = "pdb_item_";
  private static final String INDEX_PREFIX = "idx_";

  private final Jdbi jdbi;
  private final Database database;

  /**
   * Instantiates a new Pdb item table manager.
   *
   * @param jdbi     the jdbi
   * @param database the database configuration
   */
  @Inject
  public PdbItemTableManager(final Jdbi jdbi, final Database database) {
    log.info("PdbItemTableManager({}, {})", jdbi, database);
    this.jdbi = jdbi;
    this.database = database;
  }

  /**
   * Creates an item storage table for a DynamoDB table.
   *
   * @param metadata the table metadata
   */
  public void createItemTable(final PdbMetadata metadata) {
    final String itemTableName = getItemTableName(metadata.name());
    log.info("createItemTable: {}", itemTableName);

    jdbi.useHandle(handle -> {
      // Create the item table with hybrid storage approach
      final String createTableSql = buildCreateTableSql(itemTableName, metadata);
      handle.execute(createTableSql);

      // Create index on hash key for efficient lookups (best effort)
      try {
        final String indexName = INDEX_PREFIX + sanitizeTableName(metadata.name()) + "_hash";
        final String createIndexSql = String.format(
            "CREATE INDEX \"%s\" ON \"%s\" (hash_key_value)",
            indexName,
            itemTableName
        );
        handle.execute(createIndexSql);
      } catch (Exception e) {
        // Index creation is best effort - log and continue
        log.warn("Failed to create index for table {}: {}", metadata.name(), e.getMessage());
      }

      log.debug("Created item table and index for: {}", metadata.name());
    });
  }

  /**
   * Drops an item storage table.
   *
   * @param tableName the DynamoDB table name
   */
  public void dropItemTable(final String tableName) {
    final String itemTableName = getItemTableName(tableName);
    log.info("dropItemTable: {}", itemTableName);

    jdbi.useHandle(handle -> {
      final String dropTableSql = String.format(
          "DROP TABLE IF EXISTS \"%s\" CASCADE",
          itemTableName
      );
      handle.execute(dropTableSql);
      log.debug("Dropped item table: {}", itemTableName);
    });
  }

  /**
   * Gets the PostgreSQL table name for storing items.
   *
   * @param dynamoTableName the dynamo table name
   * @return the item table name
   */
  public String getItemTableName(final String dynamoTableName) {
    return TABLE_PREFIX + sanitizeTableName(dynamoTableName);
  }

  /**
   * Builds the CREATE TABLE SQL for an item storage table.
   */
  private String buildCreateTableSql(final String itemTableName, final PdbMetadata metadata) {
    // Determine if table has a sort key
    final boolean hasSortKey = metadata.sortKey().isPresent();

    // Build primary key constraint
    final String primaryKeyConstraint = hasSortKey
        ? "PRIMARY KEY (hash_key_value, sort_key_value)"
        : "PRIMARY KEY (hash_key_value)";

    // Choose JSON column type based on database
    final String jsonColumnType = database.usePostgresql() ? "JSONB" : "CLOB";

    // Quote table name for HSQLDB/PostgreSQL compatibility
    final String quotedTableName = "\"" + itemTableName + "\"";

    // Build the SQL
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s (" +
            "hash_key_value VARCHAR(2048) NOT NULL, " +
            "sort_key_value VARCHAR(2048) %s, " +
            "attributes_json %s NOT NULL, " +
            "create_date TIMESTAMP NOT NULL, " +
            "update_date TIMESTAMP NOT NULL, " +
            "%s" +
            ")",
        quotedTableName,
        hasSortKey ? "NOT NULL" : "NULL",
        jsonColumnType,
        primaryKeyConstraint
    );
  }

  /**
   * Sanitizes a table name to prevent SQL injection and ensure valid identifier.
   * Only allows alphanumeric characters, underscores, and hyphens.
   */
  private String sanitizeTableName(final String tableName) {
    return tableName.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase();
  }
}
