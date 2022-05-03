const { RedisQueueClient } = require('../dist');
const Redis = require('ioredis');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  const redis = new Redis({
    host: 'localhost',
    port: 6379,
    lazyConnect: true,
    showFriendlyErrorStack: true,
  });
  
  await redis.connect();

  const client = new RedisQueueClient({
    redis: redis,
    batchSize: 10,
    groupVisibilityTimeoutMs: 60000,
    pollingTimeoutMs: 10000,
    consumerCount: 1,
    redisKeyPrefix: `redis-ordered-queue`
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

  await sleep(5000);

  console.log('Shutting down...');
  await client.stopConsumers();
  
  await redis.disconnect(false);
}

main();
