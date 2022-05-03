import Redis from 'ioredis';
import { RedisScriptCall, prepare_redis_script, create_client_id, redis_call, clone_redis_connection } from './redis-utils';
import { RedisQueueWorker, LockHandle } from './queue-worker';
import * as scripts from './lua-scripts';

export interface RedisQueueWireMessage {
  t: number;
  c: number;
  s: number;
  d: any;
};

export interface HandleMessageCallArguments {
  data: any,
  context: {
    lock: LockHandle,
    timestamp: number | null,
    producer: number | null,
    sequence: number | null,
    latencyMs: number | null
  }
};

export type HandleMessageCall = ({ data, context: { lock } }: HandleMessageCallArguments) => Promise<void>;

export interface ConstructorArgs {
  redis: Redis.Redis;
  batchSize: number;
  groupVisibilityTimeoutMs: number;
  pollingTimeoutMs: number;
  consumerCount: number;
  redisKeyPrefix: string;
};

export class RedisQueueClient {
  private redis: Redis.Redis;
  private batchSize: number;
  private groupVisibilityTimeoutMs: number;
  private pollingTimeoutMs: number;
  private consumerCount: number;

  private groupStreamKey: string;
  private groupSetKey: string;
  private clientIndexKey: string;
  private consumerGroupId: string;
  private messagePriorityQueueKeyPrefix: string;

  private consumers: Array<RedisQueueWorker> = [];

  private clientId: number = 0;
  private lastMessageSequenceNumber: number = 0;

  private isRunning: boolean;

  private _ensured_stream_group: boolean = false;

  private callGetMetrics: RedisScriptCall | null = null;
  private callAddGroupAndMessageToQueue: RedisScriptCall | null = null;

  private statTotalInvalidMessagesCount: number = 0;
  private statLastMessageLatencies: number[] = [];
  private statLastMessageReceiveTimestamps: number[] = [];

  private _handleMessageStats (wireMessage: RedisQueueWireMessage): void {
    const now = Date.now();
    const latencyMs = Math.max(0, now - wireMessage.t);

    this.statLastMessageLatencies.push(latencyMs);
    while (this.statLastMessageLatencies.length > 100) { this.statLastMessageLatencies.shift(); }

    this.statLastMessageReceiveTimestamps.push(now);
    while (this.statLastMessageReceiveTimestamps.length > 100) { this.statLastMessageReceiveTimestamps.shift(); }
  }

  constructor ({
    redis,
    batchSize = 10,
    groupVisibilityTimeoutMs = 60000,
    pollingTimeoutMs = 10000,
    consumerCount = 10,
    redisKeyPrefix = '{redis-ordered-queue}'
  }: ConstructorArgs) {
    this.redis = redis;
    this.batchSize = batchSize;
    this.groupVisibilityTimeoutMs = groupVisibilityTimeoutMs;
    this.pollingTimeoutMs = pollingTimeoutMs;
    this.consumerCount = consumerCount;

    this.groupStreamKey = `${redisKeyPrefix}::msg-group-stream`;
    this.groupSetKey = `${redisKeyPrefix}::msg-group-set`;
    this.clientIndexKey = `${redisKeyPrefix}::consumer-index-sequence`;
    this.consumerGroupId = `${redisKeyPrefix}::consumer-group`;
    this.messagePriorityQueueKeyPrefix = `${redisKeyPrefix}::msg-group-queue::`;

    this.consumers = [];

    this.isRunning = false;

    this._ensured_stream_group = false;
  }

  private async _ensureStreamGroup (): Promise<void> {
    if (this._ensured_stream_group) return;

    this.clientId = await create_client_id(this.redis, this.clientIndexKey);

    // Ensure consumer groupId exists on the processing groupId stream
    try {
      await redis_call(this.redis, 'XGROUP', 'CREATE', this.groupStreamKey, this.consumerGroupId, 0, 'MKSTREAM');

      this._ensured_stream_group = true;
    } catch {
    }
  }

  async getMetrics ({ topMessageGroupsLimit = 10 }): Promise<any> {
    if (!this.callGetMetrics) {
      this.callGetMetrics = await prepare_redis_script(this.redis, scripts.GetMetrics, 'GetMetrics');
    }

    // Ensure consumer groupId exists on the processing groupId stream
    await this._ensureStreamGroup();

    const response = await this.callGetMetrics(
      [ this.consumerGroupId, topMessageGroupsLimit ],
      [ this.groupStreamKey, this.groupSetKey ]
    );

    function transformList(result: any[], keyName: string, valueName: string): [{ group: string, backlog: number }] {
      const out = [];
      for (let i = 0; i < result.length; i += 2) {
        out.push({
          [keyName]: result[i],
          [valueName]: parseInt(result[i + 1], 10)
        });
      }
      return out as [{ group: string, backlog: number }];
    }
    
    let sumLatency = 0;
    for (const latency of this.statLastMessageLatencies) {
      sumLatency += latency;
    }
    const avgLatency = this.statLastMessageLatencies.length > 0 ? sumLatency / this.statLastMessageLatencies.length : 0;

    return {
      bufferedMessageGroups: response[0],
      trackedMessageGroups: response[1],
      workingConsumers: response[2],
      visibleMessages: response[3],
      invalidMessages: this.statTotalInvalidMessagesCount,
      minLatencyMs: this.statLastMessageLatencies.length ? Math.min(...this.statLastMessageLatencies) : 0,
      maxLatencyMs: this.statLastMessageLatencies.length ? Math.max(...this.statLastMessageLatencies) : 0,
      avgLatencyMs: Math.round(avgLatency),
      topMessageGroups: transformList(response[4], 'group', 'backlog'),
    };
  }

  async startConsumers ({ handleMessage, handleInvalidMessage }: { handleMessage: HandleMessageCall, handleInvalidMessage?: HandleMessageCall }): Promise<void> {
    if (this.isRunning) throw new Error('Queue consumer is already running');

    this.isRunning = true;

    const callClaimTimedOutGroup = await prepare_redis_script(this.redis, scripts.ClaimTimedOutGroup, 'ClaimTimedOutGroup');
    const callUnlockGroup = await prepare_redis_script(this.redis, scripts.UnlockGroup, 'UnlockGroup');
    const callDeleteMessage = await prepare_redis_script(this.redis, scripts.DeleteMessage, 'DeleteMessage');

    // Ensure consumer groupId exists on the processing groupId stream
    await this._ensureStreamGroup();

    for (let consumerIndex = 0; consumerIndex < this.consumerCount; ++consumerIndex) {
      const consumer: RedisQueueWorker = new RedisQueueWorker({
        redis: this.redis,
        batchSize: this.batchSize,
        groupVisibilityTimeoutMs: this.groupVisibilityTimeoutMs,
        pollingTimeoutMs: this.pollingTimeoutMs,

        // Message handling callback
        handleMessage: async (msgData: any, lock: LockHandle) => {
          let wireMessage: RedisQueueWireMessage | null = null;

          try {
            wireMessage = JSON.parse(msgData.toString()) as RedisQueueWireMessage;
          } catch {}

          if (wireMessage && !!wireMessage.d && !!wireMessage.t && !!wireMessage.c && !!wireMessage.s) {
            this._handleMessageStats(wireMessage);

            return await handleMessage({
              data: wireMessage.d,
              context: {
                timestamp: wireMessage.t,
                producer: wireMessage.c,
                sequence: wireMessage.s,
                latencyMs: Math.max(0, Date.now() - wireMessage.t),
                lock
              }
            });
          } else {
            ++this.statTotalInvalidMessagesCount;

            // Bad message format
            if (handleInvalidMessage) {
              return await handleInvalidMessage({
                data: msgData,
                context: { lock, timestamp: null, producer: null, sequence: null, latencyMs: null }
              });
            } else {
              console.error('Unhandled invalid message received: ', msgData);
            }
          }
        },
        
        // Redis keys
        groupStreamKey: this.groupStreamKey,
        groupSetKey: this.groupSetKey,
        clientIndexKey: this.clientIndexKey,
        consumerGroupId: this.consumerGroupId,

        // Redis script EVALSHA hashes
        callClaimTimedOutGroup,
        callUnlockGroup,
        callDeleteMessage,

        // Priority queues key prefix generator
        createPriorityMessageQueueKey: this._createPriorityMessageQueueKey.bind(this)
      });

      this.consumers.push(consumer);
    }
  
    this.consumers.map(consumer => consumer.start());
  }

  async stopConsumers (): Promise<void> {
    if (this.isRunning) {
      await Promise.all(this.consumers.map(consumer => consumer.stop()));

      this.consumers = [];
      this.isRunning = false;
    }
  }

  async send({ data, priority = 1, groupId }: { data: any, priority: number, groupId: string }): Promise<number> {
    if (!this.callAddGroupAndMessageToQueue) {
      this.callAddGroupAndMessageToQueue = await prepare_redis_script(this.redis, scripts.AddGroupAndMessageToQueue, 'AddGroupAndMessageToQueue');
    }

    // Ensure consumer groupId exists on the processing groupId stream
    await this._ensureStreamGroup();

    const wireMessage: RedisQueueWireMessage = {
      t: Date.now(),
      c: this.clientId,
      s: ++this.lastMessageSequenceNumber,
      d: data
    };

    const [ addedMsgCount ] = await this.callAddGroupAndMessageToQueue(
      [ groupId, priority, JSON.stringify(wireMessage) ],
      [ this.groupStreamKey, this.groupSetKey, this._createPriorityMessageQueueKey(groupId) ]
    );

    return addedMsgCount;
  }

  async dispose (): Promise<void> {
    await this.stopConsumers();
  }

  private _createPriorityMessageQueueKey (groupId: string): string {
    return this.messagePriorityQueueKeyPrefix + groupId;
  }
};
