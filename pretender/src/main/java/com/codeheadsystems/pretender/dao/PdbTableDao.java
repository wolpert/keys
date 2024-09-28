package com.codeheadsystems.pretender.dao;

import java.util.List;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

/**
 * The interface Metadata dao.
 */
public interface PdbTableDao {

  /**
   * List table names list.
   *
   * @return the list
   */
  @SqlQuery("select NAME from PDB_TABLE order by ID asc")
  List<String> listTableNames();

}
