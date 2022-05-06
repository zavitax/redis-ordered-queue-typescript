const { RedisQueueClient, Redis } = require('../dist');

const logger = console;

const redis = new Redis({
  port: 6379,
  host: 'localhost',
  db: 0
});

redis.on('connect', () => { logger.info('Established a connection to redis'); main(); });
redis.on('reconnecting', time => logger.info('Reconnecting..', time));
redis.on('error', error => logger.error(error, 'Redis error'));
redis.on('close', () => logger.info('Redis server connection has closed.'));
redis.on('end', () => logger.info('Failed to reconnect to redis'));

async function handleMessage () {
  process.exit(0);
}

async function main () {
  const client = new RedisQueueClient({ redis, batchSize: 10, consumerCount: 10, groupVisibilityTimeoutMs: 60000 * 5 });

  client.startConsumers({ handleMessage });
}
