package com.codeheadsystems.pretender.dao;

import com.codeheadsystems.dbu.model.Database;
import com.codeheadsystems.pretender.model.PdbItem;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data access object for item operations.
 * Uses JDBI Handle directly to support dynamic table names.
 */
@Singleton
public class PdbItemDao {

  private static final Logger log = LoggerFactory.getLogger(PdbItemDao.class);

  private final Jdbi jdbi;
  private final Database database;

  /**
   * Instantiates a new Pdb item dao.
   *
   * @param jdbi     the jdbi
   * @param database the database configuration
   */
  @Inject
  public PdbItemDao(final Jdbi jdbi, final Database database) {
    log.info("PdbItemDao({}, {})", jdbi, database);
    this.jdbi = jdbi;
    this.database = database;
  }

  /**
   * Inserts a new item into the table.
   *
   * @param tableName the table name (without pdb_item_ prefix)
   * @param item      the item
   * @return true if inserted
   */
  public boolean insert(final String tableName, final PdbItem item) {
    log.trace("insert({}, {})", tableName, item);

    // For PostgreSQL, we need to cast JSON string to JSONB
    final String jsonValuePlaceholder = database.usePostgresql()
        ? "CAST(:attributesJson AS JSONB)"
        : ":attributesJson";

    final String sql = String.format(
        "INSERT INTO \"%s\" (hash_key_value, sort_key_value, attributes_json, create_date, update_date) " +
            "VALUES (:hashKeyValue, :sortKeyValue, %s, :createDate, :updateDate)",
        tableName,
        jsonValuePlaceholder
    );

    return jdbi.withHandle(handle ->
        handle.createUpdate(sql)
            .bind("hashKeyValue", item.hashKeyValue())
            .bind("sortKeyValue", item.sortKeyValue().orElse(null))
            .bind("attributesJson", item.attributesJson())
            .bind("createDate", item.createDate())
            .bind("updateDate", item.updateDate())
            .execute() > 0
    );
  }

  /**
   * Gets an item by its primary key.
   *
   * @param tableName    the table name
   * @param hashKeyValue the hash key value
   * @param sortKeyValue the sort key value (optional)
   * @return the item
   */
  public Optional<PdbItem> get(final String tableName,
                                final String hashKeyValue,
                                final Optional<String> sortKeyValue) {
    log.trace("get({}, {}, {})", tableName, hashKeyValue, sortKeyValue);

    final String sql = sortKeyValue.isPresent()
        ? String.format(
            "SELECT * FROM \"%s\" WHERE hash_key_value = :hashKey AND sort_key_value = :sortKey",
            tableName
        )
        : String.format(
            "SELECT * FROM \"%s\" WHERE hash_key_value = :hashKey",
            tableName
        );

    return jdbi.withHandle(handle -> {
      var query = handle.createQuery(sql)
          .bind("hashKey", hashKeyValue);

      sortKeyValue.ifPresent(sk -> query.bind("sortKey", sk));

      return query.map((rs, ctx) -> {
        final PdbItem item = com.codeheadsystems.pretender.model.ImmutablePdbItem.builder()
            .tableName(tableName)
            .hashKeyValue(rs.getString("hash_key_value"))
            .sortKeyValue(rs.getString("sort_key_value") != null ?
                Optional.of(rs.getString("sort_key_value")) : Optional.empty())
            .attributesJson(rs.getString("attributes_json"))
            .createDate(rs.getTimestamp("create_date").toInstant())
            .updateDate(rs.getTimestamp("update_date").toInstant())
            .build();
        return item;
      }).findFirst();
    });
  }

  /**
   * Updates an existing item.
   *
   * @param tableName the table name
   * @param item      the updated item
   * @return true if updated
   */
  public boolean update(final String tableName, final PdbItem item) {
    log.trace("update({}, {})", tableName, item);

    // For PostgreSQL, we need to cast JSON string to JSONB
    final String jsonValuePlaceholder = database.usePostgresql()
        ? "CAST(:attributesJson AS JSONB)"
        : ":attributesJson";

    final String sql = item.sortKeyValue().isPresent()
        ? String.format(
            "UPDATE \"%s\" SET attributes_json = %s, update_date = :updateDate " +
                "WHERE hash_key_value = :hashKeyValue AND sort_key_value = :sortKeyValue",
            tableName,
            jsonValuePlaceholder
        )
        : String.format(
            "UPDATE \"%s\" SET attributes_json = %s, update_date = :updateDate " +
                "WHERE hash_key_value = :hashKeyValue",
            tableName,
            jsonValuePlaceholder
        );

    return jdbi.withHandle(handle ->
        handle.createUpdate(sql)
            .bind("hashKeyValue", item.hashKeyValue())
            .bind("sortKeyValue", item.sortKeyValue().orElse(null))
            .bind("attributesJson", item.attributesJson())
            .bind("updateDate", item.updateDate())
            .execute() > 0
    );
  }

  /**
   * Deletes an item by its primary key.
   *
   * @param tableName    the table name
   * @param hashKeyValue the hash key value
   * @param sortKeyValue the sort key value (optional)
   * @return true if deleted
   */
  public boolean delete(final String tableName,
                        final String hashKeyValue,
                        final Optional<String> sortKeyValue) {
    log.trace("delete({}, {}, {})", tableName, hashKeyValue, sortKeyValue);

    final String sql = sortKeyValue.isPresent()
        ? String.format(
            "DELETE FROM \"%s\" WHERE hash_key_value = :hashKey AND sort_key_value = :sortKey",
            tableName
        )
        : String.format(
            "DELETE FROM \"%s\" WHERE hash_key_value = :hashKey",
            tableName
        );

    return jdbi.withHandle(handle -> {
      var update = handle.createUpdate(sql)
          .bind("hashKey", hashKeyValue);

      sortKeyValue.ifPresent(sk -> update.bind("sortKey", sk));

      return update.execute() > 0;
    });
  }

  /**
   * Queries items by hash key with optional sort key condition.
   *
   * @param tableName        the table name
   * @param hashKeyValue     the hash key value
   * @param sortKeyCondition the sort key SQL condition (e.g., "sort_key_value = :sortKey")
   * @param sortKeyValue     the sort key value for binding
   * @param limit            the maximum number of items to return
   * @return the list of items
   */
  public List<PdbItem> query(final String tableName,
                             final String hashKeyValue,
                             final String sortKeyCondition,
                             final Optional<String> sortKeyValue,
                             final int limit) {
    log.trace("query({}, {}, {}, {}, {})", tableName, hashKeyValue, sortKeyCondition, sortKeyValue, limit);

    final String whereClause = sortKeyCondition != null && !sortKeyCondition.isBlank()
        ? "hash_key_value = :hashKey AND " + sortKeyCondition
        : "hash_key_value = :hashKey";

    final String sql = String.format(
        "SELECT * FROM \"%s\" WHERE %s LIMIT :limit",
        tableName,
        whereClause
    );

    return jdbi.withHandle(handle -> {
      var query = handle.createQuery(sql)
          .bind("hashKey", hashKeyValue)
          .bind("limit", limit);

      sortKeyValue.ifPresent(sk -> query.bind("sortKey", sk));

      return query.map((rs, ctx) -> {
        final PdbItem item = com.codeheadsystems.pretender.model.ImmutablePdbItem.builder()
            .tableName(tableName)
            .hashKeyValue(rs.getString("hash_key_value"))
            .sortKeyValue(rs.getString("sort_key_value") != null ?
                Optional.of(rs.getString("sort_key_value")) : Optional.empty())
            .attributesJson(rs.getString("attributes_json"))
            .createDate(rs.getTimestamp("create_date").toInstant())
            .updateDate(rs.getTimestamp("update_date").toInstant())
            .build();
        return item;
      }).list();
    });
  }

  /**
   * Scans all items in a table.
   *
   * @param tableName the table name
   * @param limit     the maximum number of items to return
   * @return the list of items
   */
  public List<PdbItem> scan(final String tableName, final int limit) {
    log.trace("scan({}, {})", tableName, limit);

    final String sql = String.format(
        "SELECT * FROM \"%s\" LIMIT :limit",
        tableName
    );

    return jdbi.withHandle(handle ->
        handle.createQuery(sql)
            .bind("limit", limit)
            .map((rs, ctx) -> {
              final PdbItem item = com.codeheadsystems.pretender.model.ImmutablePdbItem.builder()
                  .tableName(tableName)
                  .hashKeyValue(rs.getString("hash_key_value"))
                  .sortKeyValue(rs.getString("sort_key_value") != null ?
                      Optional.of(rs.getString("sort_key_value")) : Optional.empty())
                  .attributesJson(rs.getString("attributes_json"))
                  .createDate(rs.getTimestamp("create_date").toInstant())
                  .updateDate(rs.getTimestamp("update_date").toInstant())
                  .build();
              return item;
            })
            .list()
    );
  }
}
