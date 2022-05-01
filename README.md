# redis-ordered-queue

## What is it?

Redis-based ordered queue with support for message priority.

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
import Redis from 'ioredis';
import { RedisQueueClient } from 'redis-ordered-queue';

function sleep(ms: number): Promise<void> {
  return new Promise<void>(resolve => {
    setTimeout(resolve, ms);
  });
}

async function main(): Promise<void> {
  const client: RedisQueueClient = new RedisQueueClient({
    redis: new Redis({ host: 'localhost', port: 6379 }),
    batchSize: 10,
    groupVisibilityTimeoutMs: 60000,
    pollingTimeoutMs: 10000,
    consumerCount: 10,
    redisKeyPrefix: '{redis-ordered-queue}'
  });

  async function handleMessage({ data, context: { lock: { groupId } } }: { data: any, context: { lock: { groupId: string } } }): Promise<void> {
    console.log('Received message from group ', groupId, ': ', data);
  }

  await client.send({
    data: { my: "message" },
    priority: 1,
    groupId: 'message-group-1'
  });

  const metrics = await client.getMetrics(10);

  console.log('metrics: ', metrics);

  await client.startConsumers(handleMessage);
  sleep(5000);
  await client.stopConsumers();
}

main();
```
