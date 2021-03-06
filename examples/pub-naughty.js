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

function data(groupId) {
  this.seq = (this.seq || 0) + 1;

  return { group: groupId, seq: this.seq, ts: Date.now().valueOf() };
}

async function main () {
  const client = new RedisQueueClient({ redis, batchSize: 1, messageGroupLockTimeoutSeconds: 60 });

  const groups = [ ];

  for (let groupIndex = 1; groupIndex <= 1; ++groupIndex) {
    groups.push(`G${groupIndex}`);
  }

  for (let i = 0; i < 100000 * groups.length; ++i) {
    const groupIndex = Math.floor(Math.random() * groups.length);
    const groupId = groups[groupIndex];

    const messagePriority = Math.round(10 * Math.random());

    await client.send({ data: data(groupId), priority: messagePriority, groupId: groupId });

    if (i % 1000 === 0) {
      console.log(`SENT NAUGHTY MSGS TO SAME GROUP: ${i}`);
    }
  }

  redis.disconnect();
}
