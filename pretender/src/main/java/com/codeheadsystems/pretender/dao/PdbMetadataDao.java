package com.codeheadsystems.pretender.dao;

import com.codeheadsystems.pretender.model.PdbMetadata;
import java.util.List;
import java.util.Optional;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/**
 * The interface PdbMetadata dao.
 */
public interface PdbMetadataDao {

  /**
   * List table names list.
   *
   * @return the list
   */
  @SqlQuery("select NAME from PDB_TABLE order by NAME asc")
  List<String> listTableNames();

  /**
   * Gets table.
   *
   * @param name the name
   * @return the table
   */
  @SqlQuery("select * from PDB_TABLE where NAME = :name")
  Optional<PdbMetadata> getTable(@Bind("name") String name);

  /**
   * Insert boolean.
   *
   * @param pdbMetadata the pdb table
   * @return the boolean
   */
  @SqlUpdate("insert into PDB_TABLE (NAME, HASH_KEY, SORT_KEY, GLOBAL_SECONDARY_INDEXES, TTL_ATTRIBUTE_NAME, TTL_ENABLED, CREATE_DATE) "
      + "values (:name, :hashKey, :sortKey, :globalSecondaryIndexes, :ttlAttributeName, :ttlEnabled, :createDate)")
  boolean insert(@BindPojo PdbMetadata pdbMetadata);

  /**
   * Delete boolean.
   *
   * @param name the name
   * @return the boolean
   */
  @SqlUpdate("delete from PDB_TABLE where NAME = :name")
  boolean delete(@Bind("name") String name);

  /**
   * Update TTL settings.
   *
   * @param name              the table name
   * @param ttlAttributeName the TTL attribute name
   * @param ttlEnabled       whether TTL is enabled
   * @return the boolean
   */
  @SqlUpdate("update PDB_TABLE set TTL_ATTRIBUTE_NAME = :ttlAttributeName, TTL_ENABLED = :ttlEnabled where NAME = :name")
  boolean updateTtl(@Bind("name") String name,
                    @Bind("ttlAttributeName") String ttlAttributeName,
                    @Bind("ttlEnabled") boolean ttlEnabled);

}
