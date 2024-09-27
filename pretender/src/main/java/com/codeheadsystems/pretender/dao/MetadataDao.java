package com.codeheadsystems.pretender.dao;

import java.util.List;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

/**
 * The interface Metadata dao.
 */
public interface MetadataDao {

  /**
   * List table names list.
   *
   * @return the list
   */
  @SqlQuery("select ID from PDB_METADATA order by ID asc")
  List<String> listTableNames();

}
