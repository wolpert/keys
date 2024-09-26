package com.codeheadsystems.pretender;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbPretenderClient implements DynamoDbClient {

  @Override
  public String serviceName() {
    return "dynamodb";
  }

  @Override
  public void close() {

  }
}
