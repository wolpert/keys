package com.codeheadsystems.pretender.manager;

import com.codeheadsystems.pretender.converter.MetaDataConverter;
import com.codeheadsystems.pretender.dao.MetadataDao;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Metadata manager.
 */
@Singleton
public class MetadataManager {

  private static final Logger log = LoggerFactory.getLogger(MetadataManager.class);

  private final MetadataDao metadataDao;
  private final MetaDataConverter metaDataConverter;

  /**
   * Instantiates a new Metadata manager.
   *
   * @param jdbi              the jdbi
   * @param metaDataConverter the meta data converter
   */
  @Inject
  public MetadataManager(final Jdbi jdbi,
                         final MetaDataConverter metaDataConverter) {
    this.metaDataConverter = metaDataConverter;
    log.info("MetadataManager({})", jdbi);
    this.metadataDao = jdbi.onDemand(MetadataDao.class);
  }

}
