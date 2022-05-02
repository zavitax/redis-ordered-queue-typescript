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

function sleep (ms = 1000) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  })
}

const g_valMap = {};

function aavg (arr) {
  let sum = 0;
  for (const val of arr) {
    sum += val;
  }
  return sum / arr.length;
}

async function handleMessage ({ data, context: { lock: { groupId, consumerId }, timestamp } }) {
  const delta = Date.now().valueOf() - timestamp;

  this.total = (this.total || 0) + 1;
  this.ts = (this.ts || []);

  this.ts.push(delta);

  const key = groupId;
  const id = consumerId;

  if (g_valMap[key]) {
    if (g_valMap[key] === id) {
      console.log(`    * [${id}]: same consumer msg: `, key);
    } else {
      console.error(`    * [${id}]: different consumer has msg for same ordering key: `, key, ' -> ', g_valMap[key]);
    }
  }

  g_valMap[key] = id;

  if (this.total % 100 === 0) {
    console.log(`GOT MSGS: ${this.total}    min: ${Math.min(...ts)}    max: ${Math.max(...ts)}    avg: ${aavg(ts)}`);
    //console.log(`GOT MSGS: ${this.total}`);

    this.ts = [];
  }
  await sleep(Math.ceil(Math.random() * 500));

  delete g_valMap[key];
}

async function main () {
  const client = new RedisQueueClient({ redis, batchSize: 10, consumerCount: 500, groupVisibilityTimeoutMs: 60000 * 5 });

  client.startConsumers({ handleMessage });
}
