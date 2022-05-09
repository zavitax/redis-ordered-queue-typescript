const { RedisQueueClient } = require('../dist');
const { RedisQueueWorker } = require('../dist/queue-worker');
const Redis = require('ioredis');

async function sleep (ms = 1000) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('RedisQueueClient', () => {
  let queue;
  let redis;

  beforeEach(async () => {
    redis = new Redis({
      host: 'localhost',
      port: 6379,
      lazyConnect: true,
      showFriendlyErrorStack: true,
    });

    await redis.connect();

    queue = new RedisQueueClient({
      redis: redis,
      batchSize: 10,
      groupVisibilityTimeoutMs: 1000,
      pollingTimeoutMs: 1000,
      consumerCount: 1,
      redisKeyPrefix: `{redis-ordered-queue-test}`
    });
  });

  afterEach(async () => {
    jest.clearAllMocks();

    // Cleanup Redis
    // await redis.send_command('FLUSHDB');
    redis.disconnect(false);
  });

  describe('create', () => {
    const connectSpy = jest.spyOn(Redis.prototype, 'connect');

    it('should create a new instance', async () => {
      expect(queue).toBeInstanceOf(RedisQueueClient);
      expect(connectSpy).toHaveBeenCalled();
    });
  });

  describe('enqueue', () => {
    const sendSpy = jest.spyOn(RedisQueueClient.prototype, 'send');

    it('should enqueue data', async () => {
      const data = { key: 'value' };
      await expect(queue.send({ data, priority: 1, groupId: 'test-group' })).resolves.not.toThrowError();
      expect(sendSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('start', () => {
    const handleMessageSpy = jest.fn();
    
    const startConsumersSpy = jest.spyOn(RedisQueueClient.prototype, 'startConsumers');

    afterEach(async () => {
      await queue.stopConsumers();
    });

    it('should receive messages', async () => {
      const data = { key: 'value' };

      await expect(queue.startConsumers({ handleMessage: handleMessageSpy })).resolves.not.toThrowError();
      await queue.send({ data, priority: 1, groupId: 'test-group' });

      await sleep(1000);

      expect(startConsumersSpy).toHaveBeenCalledTimes(1);
      expect(handleMessageSpy).toHaveBeenCalled();
    });

    it('should stop messages messages after handleMessage error', async () => {
      const failHandleMessageSpy = jest.fn(cb => () => { throw new Error("error"); });

      const data = { key: 'value' };

      await expect(queue.startConsumers({ handleMessage: failHandleMessageSpy })).resolves.not.toThrowError();
      await queue.send({ data, priority: 1, groupId: 'test-group' });

      await sleep(1000);

      expect(startConsumersSpy).toHaveBeenCalledTimes(1);
      expect(failHandleMessageSpy).toHaveBeenCalled();
    });
  });

  describe('dispose', () => {
    const stopConsumersSpy = jest.spyOn(RedisQueueClient.prototype, 'stopConsumers');

    it('should stop listening and disconnect from Redis', async () => {
      await expect(queue.stopConsumers()).resolves.not.toThrowError();
      expect(stopConsumersSpy).toHaveBeenCalled();
    });
  });
});
