package com.codeheadsystems.pretender.dagger;

import com.codeheadsystems.pretender.DynamoDbPretenderClient;
import com.codeheadsystems.pretender.DynamoDbStreamsPretenderClient;
import com.codeheadsystems.pretender.manager.PdbTableManager;
import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.service.StreamCleanupService;
import com.codeheadsystems.pretender.service.TtlCleanupService;
import dagger.Component;
import javax.inject.Singleton;

/**
 * The interface Pretender component.
 */
@Singleton
@Component(modules = {PretenderModule.class, ConfigurationModule.class, CommonModule.class})
public interface PretenderComponent {

  /**
   * Instance pretender component.
   *
   * @param configuration the configuration
   * @return the pretender component
   */
  static PretenderComponent instance(final Configuration configuration) {
    return DaggerPretenderComponent.builder().configurationModule(new ConfigurationModule(configuration)).build();
  }

  /**
   * Pretender database manager pretender database manager.
   *
   * @return the pretender database manager
   */
  DynamoDbPretenderClient dynamoDbPretenderClient();

  /**
   * PDB table manager.
   *
   * @return the pdb table manager
   */
  PdbTableManager pdbTableManager();

  /**
   * TTL cleanup service.
   *
   * @return the ttl cleanup service
   */
  TtlCleanupService ttlCleanupService();

  /**
   * DynamoDB Streams pretender client.
   *
   * @return the dynamodb streams pretender client
   */
  DynamoDbStreamsPretenderClient dynamoDbStreamsPretenderClient();

  /**
   * Stream cleanup service.
   *
   * @return the stream cleanup service
   */
  StreamCleanupService streamCleanupService();
}
