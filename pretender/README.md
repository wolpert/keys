# Pretender

Provides for a AWS DynamoDB compatible library that works with Java.
The resulting data is stored in a SQL database, primarily designed for
PostgreSQL but not limited to it.

# _WARNING_: This was built with Claude

Slop warning... I'm in the process of reviewing the code, but consider it
experimental until I can verify it works as intended and is well designed.

## Idea

The ability to run DynamoDB locally for development that is provided
by Amazon's [local dynamodb project](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html) is great feature.
It uses a SQLite instance to store the data, mimicking how DDB works on AWS.
However, there are a few shortcomings that it does not address.

1. It is useful during development to have easy access to the datastore. They do not give that.
2. If you are a startup, it may be a while before you can fully realize the benefit of running in the cloud.

## Drawbacks

1. It is not the complete DynamoDB implementation of the API. 
2. Features in the real service like DynamoDB streams or replication do not exist.
3. 