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
  @SqlUpdate("insert into PDB_TABLE (NAME, HASH_KEY, SORT_KEY, CREATE_DATE) values (:name, :hashKey, :sortKey, :createDate)")
  boolean insert(@BindPojo PdbMetadata pdbMetadata);

  /**
   * Delete boolean.
   *
   * @param name the name
   * @return the boolean
   */
  @SqlUpdate("delete from PDB_TABLE where NAME = :name")
  boolean delete(@Bind("name") String name);

}
