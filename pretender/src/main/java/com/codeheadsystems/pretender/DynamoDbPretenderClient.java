package com.codeheadsystems.pretender;

import com.codeheadsystems.pretender.factory.JdbiFactory;
import com.codeheadsystems.pretender.liquibase.LiquibaseHelper;
import com.codeheadsystems.pretender.manager.PretenderDatabaseManager;
import com.codeheadsystems.pretender.model.Configuration;
import org.jdbi.v3.core.Jdbi;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * The type Dynamo db pretender client.
 */
public class DynamoDbPretenderClient implements DynamoDbClient {

  private static final String SERVICE_NAME = "dynamodb";
  private final PretenderDatabaseManager manager;

  private DynamoDbPretenderClient(final Builder builder) {
    final Jdbi jdbi = new JdbiFactory(builder.configuration).createJdbi();
    if (builder.runLiquibase) {
      new LiquibaseHelper().runLiquibase(jdbi);
    }
    this.manager = new PretenderDatabaseManager(jdbi);
  }

  private static Builder builder() {
    return new Builder();
  }

  @Override
  public String serviceName() {
    return SERVICE_NAME;
  }

  @Override
  public void close() {

  }

  /**
   * The type Builder.
   */
  static class Builder {

    private Configuration configuration;
    private boolean runLiquibase = true;

    private Builder() {

    }

    /**
     * With configuration builder.
     *
     * @param configuration the configuration
     * @return the builder
     */
    public Builder withConfiguration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * With run liquibase builder.
     *
     * @param runLiquibase the run liquibase
     * @return the builder
     */
    public Builder withRunLiquibase(boolean runLiquibase) {
      this.runLiquibase = runLiquibase;
      return this;
    }

    /**
     * Build dynamo db pretender client.
     *
     * @return the dynamo db pretender client
     */
    public DynamoDbPretenderClient build() {
      return new DynamoDbPretenderClient(this);
    }

  }

}
