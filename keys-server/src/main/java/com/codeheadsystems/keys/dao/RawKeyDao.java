package com.codeheadsystems.keys.dao;

import com.codeheadsystems.keys.model.RawKey;
import java.util.List;
import java.util.Optional;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/**
 * The interface PdbMetadata dao.
 */
public interface RawKeyDao {

  /**
   * List keys list.
   *
   * @return the list
   */
  @SqlQuery("select UUID from KEYS order by UUID asc")
  List<String> listKeys();

  /**
   * Gets key.
   *
   * @param uuid the uuid
   * @return the key
   */
  @SqlQuery("select * from KEYS where UUID = :uuid")
  Optional<RawKey> getKey(@Bind("uuid") String uuid);

  /**
   * Insert boolean.
   *
   * @param rawKey the raw key
   * @return the boolean
   */
  @SqlUpdate("insert into KEYS (UUID, KEY) values (:uuid, :key, :size)")
  boolean insert(@BindPojo RawKey rawKey);

  /**
   * Delete boolean.
   *
   * @param uuid the uuid
   * @return the boolean
   */
  @SqlUpdate("delete from KEYS where UUID = :uuid")
  boolean delete(@Bind("uuid") String uuid);

}
