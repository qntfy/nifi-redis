# nifi-redis 
[![Latest Release](https://img.shields.io/github/release/qntfy/nifi-redis.svg)](https://github.com/qntfy/nifi-redis/releases/latest)
[![Software License](https://img.shields.io/github/license/qntfy/nifi-redis.svg)](LICENSE.md)
[![Build Status](https://travis-ci.org/qntfy/nifi-redis.svg?branch=master)](https://travis-ci.org/qntfy/nifi-redis)
[![Total Downloads](https://img.shields.io/github/downloads/qntfy/nifi-redis/total.svg)](https://github.com/qntfy/nifi-redis/releases)



NiFi Processors for handling data in Redis

## Processors
### GetRedis
This processor is currently incomplete, but will be a simple Redis source, much like GetKafka. 
New FlowFiles will be created from the contents of a Redis key(s).

### GetRedisEnrichment
This processor is under active development. 
The processor accepts FlowFiles and then looks at the contents of a Redis hash based on the processor's configured "topic" and the UUID of the flowfile.
If the specified number of values are in the redis hash for the given flowfile, the contents from Redis will either be added as attributes to the FlowFile or used to enrich the content. 
Currently, only enrichment of JSON is supported.

### PutRedis
This processor is currently completed.

Before using this processor in your project, you'd better make some modifications on the source code and build a nar file to satisfy your requirement.

### PutRedisCluster
This processor is currently incomplete.

#### Configuration Options
- **Redis Connection String**: The Connection String to use in order to connect to Redis.
- **Client Name**: Client Name to use when communicating with Redis
- **Topic**: Prefix of keys to gather data from, i.e. $topic:$flowfileUUID
- **Enrichments**: Number of required enrichments in the key's hash
- **JSON Content Mode**: If TRUE, add the enrichment data to the JSON, otherwise add as FlowFile attributes
- **JSON Enrichment Field**: If in JSON Content Mode, add the enrichments to this top-level JSON field
