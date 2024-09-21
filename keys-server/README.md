# Keys server

A simple server to get/set identified keys for a given consumer.
Will include oauth2 support to identify the consumer, and allows the
consumer to generate large numbers of named/versioned keys.

## Test local execution

```bash
gradle :keys-server:run --args="server config.yml"
```