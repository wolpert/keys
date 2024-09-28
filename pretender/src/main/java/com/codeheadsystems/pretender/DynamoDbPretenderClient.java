package com.codeheadsystems.pretender;

import com.codeheadsystems.pretender.converter.PdbTableConverter;
import com.codeheadsystems.pretender.manager.PdbTableManager;
import com.codeheadsystems.pretender.manager.PretenderDatabaseManager;
import com.codeheadsystems.pretender.model.PdbTable;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/**
 * The type Dynamo db pretender client.
 */
@Singleton
public class DynamoDbPretenderClient implements DynamoDbClient {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbPretenderClient.class);

  private static final String SERVICE_NAME = "dynamodb";
  private final PretenderDatabaseManager pretenderDatabaseManager;
  private final PdbTableManager pdbTableManager;
  private final PdbTableConverter pdbTableConverter;

  /**
   * Instantiates a new Dynamo db pretender client.
   *
   * @param pretenderDatabaseManager the pretender database manager
   * @param pdbTableManager          the metadata manager
   * @param pdbTableConverter        the pdb table converter
   */
  @Inject
  public DynamoDbPretenderClient(final PretenderDatabaseManager pretenderDatabaseManager,
                                 final PdbTableManager pdbTableManager,
                                 final PdbTableConverter pdbTableConverter) {
    log.info("DynamoDbPretenderClient({},{},{})", pretenderDatabaseManager, pdbTableManager, pdbTableConverter);
    this.pdbTableManager = pdbTableManager;
    this.pretenderDatabaseManager = pretenderDatabaseManager;
    this.pdbTableConverter = pdbTableConverter;
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
    return ListTablesResponse.builder().tableNames(pdbTableManager.listPdbTables()).build();
  }

  @Override
  public CreateTableResponse createTable(final CreateTableRequest createTableRequest) throws AwsServiceException, SdkClientException {
    final PdbTable table = pdbTableConverter.convert(createTableRequest);
    if (pdbTableManager.insertPdbTable(table)) {
      return CreateTableResponse.builder().tableDescription(pdbTableConverter.fromPdbTable(table)).build();
    } else {
      throw ResourceInUseException.builder().message("Table already exists").build();
    }
  }

  @Override
  public DeleteTableResponse deleteTable(final DeleteTableRequest deleteTableRequest) throws AwsServiceException, SdkClientException {
    final String tableName = deleteTableRequest.tableName();
    final Optional<PdbTable> pdbTable = pdbTableManager.getPdbTable(tableName);
    if (pdbTable.isEmpty()) {
      throw ResourceNotFoundException.builder().message("Table not found").build();
    }
    if (pdbTableManager.deletePdbTable(tableName)) {
      return DeleteTableResponse.builder().tableDescription(pdbTableConverter.fromPdbTable(pdbTable.get())).build();
    } else {
      throw InternalServerErrorException.builder().message("Unable to delete table").build();
    }
  }
}
