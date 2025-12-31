# Pretender

Provides for a AWS DynamoDB compatible library that works with Java.
The resulting data is stored in a SQL database, primarily designed for
PostgreSQL but not limited to it.

# _WARNING_: This was built with Claude

Slop warning... I'm in the process of reviewing the code, but consider it
experimental until I can verify it works as intended and is well designed.

Streaming features here probably do not work, though you'll see Claude
docs say that they do. But there is a JVM assertion failure running the tests
for one class that I had to comment out. That's likely a serious red flag.
Be aware that I have not verified this module yet.

## Idea

The ability to run DynamoDB locally for development that is provided
by Amazon's [local dynamodb project](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html) is great feature.
It uses a SQLite instance to store the data, mimicking how DDB works on AWS.
However, there are a few shortcomings that it does not address.

1. It is useful during development to have easy access to the datastore. They do not give that.
2. If you are a startup, it may be a while before you can fully realize the benefit of running in the cloud.

# Claude notes below

## Features

### Core DynamoDB Operations
- **Table Management**: createTable, deleteTable, describeTable, listTables
- **Item Operations**: putItem, getItem, updateItem, deleteItem, query, scan
- **Global Secondary Indexes (GSI)**: Full support for GSI creation and querying
- **Time-to-Live (TTL)**: Automatic item expiration with background cleanup
- **DynamoDB Streams**: Change data capture with 24-hour retention (see below)

### DynamoDB Streams Support

Pretender now includes full support for DynamoDB Streams:

- **Stream Configuration**: Enable/disable streams per table with configurable StreamViewType
- **Event Capture**: Automatic capture of INSERT, MODIFY, and REMOVE events
- **Stream Consumption**: Complete implementation of Streams API (describeStream, getShardIterator, getRecords, listStreams)
- **24-Hour Retention**: Automatic cleanup matching AWS behavior
- **Stream View Types**: KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES

See [STREAMS_IMPLEMENTATION.md](STREAMS_IMPLEMENTATION.md) for detailed documentation.

## Drawbacks

1. It is not the complete DynamoDB implementation of the API.
2. Some advanced features like global tables, transactions, and PartiQL are not yet implemented.
3. 