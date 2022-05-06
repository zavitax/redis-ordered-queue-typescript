const { RedisQueueClient } = require('../dist');
const Redis = require('ioredis');

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

function sleep (ms = 1000) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  })
}

async function main () {
  const client = new RedisQueueClient({
    redis,
    batchSize: 1,
    messageGroupLockTimeoutSeconds: 5,
 });

 require('readline').emitKeypressEvents(process.stdin);
 process.stdin.setRawMode(true);

 let isRunning = true;

 process.stdin.on('keypress', (str, key) => {
  if (key.ctrl && key.name === 'c') {
    isRunning = false;

    console.log('Shutting down...');
  }
 });

 while (isRunning) {
    const metrics = await client.getMetrics({ topMessageGroupsLimit: 3 });

    console.log(`METRICS: `, metrics);

    await sleep(1000);
  }

  redis.disconnect(false);

  process.exit(0);
}
