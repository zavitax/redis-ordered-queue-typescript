# redis-ordered-queue

## What is it?

Redis-based ordered queue with support for message priority.

## Quality metrics

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=bugs)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-typescript&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-typescript)

## Queue guarantees

### Low latency delivery

Based on traffic and amount of consumers, it is possible to reach very low delivery latency. I observed 2-15 millisecond latencies with thousands of consumers, message groups and messages.

### Equal attention to all message groups

Message groups are cycled through, each consumer looking at the next available message group in the buffer.

### No conflicts between message groups

Message groups are locked for processing before a consumer can process messages associated with that message group. This ensures that when a consumer is processing messages in a message group, no other consumers can see messages in the same message group.

The drawback is that if you only have one message group in your set-up, only one consumer can be active at each moment.

### At least once delivery

Message redelivery attempts will take place until it's acknowledged by the consumer.

### High performance, low memory footprint

The amount of consumers you can run on each worker node is limited only by the amount of allowed connections on your Redis server.

## Infrastructure

The library leverages `ioredis` for communication with the Redis server.

## Usage

```typescript
const { RedisQueueClient } = require('../dist');
const Redis = require('ioredis');

async function main() {
  const redis = new Redis({ host: 'localhost', port: 6379 });

  const client = new RedisQueueClient({
    redis: redis,
    batchSize: 10,
    groupVisibilityTimeoutMs: 60000,
    pollingTimeoutMs: 10000,
    consumerCount: 1,
    redisKeyPrefix: 'redis-ordered-queue'
  });

  async function handleMessage({ data, context: { lock: { groupId } } }) {
    console.log('Received message from group ', groupId, ': ', data);
  }

  async function handleInvalidMessage({ data, context: { lock: { groupId }} }) {
    console.log('Invalid messsage: ', groupId, ': ', data);
  }

  await client.startConsumers({ handleMessage, handleInvalidMessage });

  await client.send({
    data: { my: "message 1" },
    priority: 1,
    groupId: 'message-group-1'
  });

  await client.send({
    data: { my: "message 2" },
    priority: 1,
    groupId: 'message-group-1'
  });

  await client.send({
    data: { my: "message 3" },
    priority: 1,
    groupId: 'message-group-1'
  });

  console.log('metrics: ', await client.getMetrics(10));

  console.log('Shutting down...');
  await client.stopConsumers();

  await redis.disconnect(false);
}

main();
```
