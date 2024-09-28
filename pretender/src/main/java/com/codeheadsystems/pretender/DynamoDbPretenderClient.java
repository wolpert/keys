package com.codeheadsystems.pretender;

import com.codeheadsystems.pretender.manager.PdbTableManager;
import com.codeheadsystems.pretender.manager.PretenderDatabaseManager;
import javax.inject.Inject;
import javax.inject.Singleton;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/**
 * The type Dynamo db pretender client.
 */
@Singleton
public class DynamoDbPretenderClient implements DynamoDbClient {

  private static final String SERVICE_NAME = "dynamodb";
  private final PretenderDatabaseManager pretenderDatabaseManager;
  private final PdbTableManager pdbTableManager;

  /**
   * Instantiates a new Dynamo db pretender client.
   *
   * @param pretenderDatabaseManager the pretender database manager
   * @param pdbTableManager          the metadata manager
   */
  @Inject
  public DynamoDbPretenderClient(final PretenderDatabaseManager pretenderDatabaseManager,
                                 final PdbTableManager pdbTableManager) {
    this.pdbTableManager = pdbTableManager;
    this.pretenderDatabaseManager = pretenderDatabaseManager;
  }

  @Override
  public String serviceName() {
    return SERVICE_NAME;
  }

  @Override
  public void close() {

  }

  @Override
  public ListTablesResponse listTables(final ListTablesRequest listTablesRequest) throws AwsServiceException, SdkClientException {
    return DynamoDbClient.super.listTables(listTablesRequest);
  }

  @Override
  public CreateTableResponse createTable(final CreateTableRequest createTableRequest) throws AwsServiceException, SdkClientException {
    return DynamoDbClient.super.createTable(createTableRequest);
  }

}
