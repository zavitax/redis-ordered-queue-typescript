import { EventEmitter } from 'eventemitter3';
import Redis from 'ioredis';
import { RedisScriptCall, clone_redis_connection } from './redis-utils';

export type HandleConsumedMessageCall = (messageData: any, lock: LockHandle) => Promise<void>;

export interface LockHandle {
  groupId: string;
  messageId: string;
  queueId: string;
  consumerId: string;
};

export interface ConstructorArgs {
  redis: Redis.Redis;
  batchSize: number;
  groupVisibilityTimeoutMs: number;
  pollingTimeoutMs: number;
  handleMessage: HandleConsumedMessageCall;

  // Redis keys
  groupStreamKey: string;
  groupSetKey: string;
  consumerGroupId: string;

  // Redis script EVALSHA hashes
  callClaimTimedOutGroup: RedisScriptCall;
  callUnlockGroup: RedisScriptCall;
  callDeleteMessage: RedisScriptCall;

  createPriorityMessageQueueKey: (groupId: string) => string;
};

export interface Events {
  stopped (): void;
};

export class RedisQueueWorker extends EventEmitter<Events> {
  private parentRedis: Redis.Redis;
  private redis: Redis.Redis;
  private batchSize: number;
  private groupVisibilityTimeoutMs: number;
  private pollingTimeoutMs: number;
  private handleMessage: HandleConsumedMessageCall;

  // Redis keys
  private groupStreamKey: string;
  private groupSetKey: string;
  private consumerGroupId: string;

  // Priority queues keys prefix
  private _createPriorityMessageQueueKey: (groupId: string) => string;

  private consumerId: string;

  // Redis script EVALSHA hashes
  private callClaimTimedOutGroup: RedisScriptCall;
  private callUnlockGroup: RedisScriptCall;
  private callDeleteMessage: RedisScriptCall;

  private isRunning: boolean = false;

constructor ({
    redis,
    batchSize,
    groupVisibilityTimeoutMs,
    pollingTimeoutMs,
    handleMessage,

    // Redis keys
    groupStreamKey,
    groupSetKey,
    consumerGroupId,

    // Redis script EVALSHA hashes
    callClaimTimedOutGroup,
    callUnlockGroup,
    callDeleteMessage,

    createPriorityMessageQueueKey
}: ConstructorArgs) {
    super();

    this.parentRedis = redis;
    
    this.batchSize = batchSize;
    this.groupVisibilityTimeoutMs = groupVisibilityTimeoutMs;
    this.pollingTimeoutMs = pollingTimeoutMs;
    this.handleMessage = handleMessage;
    
    this.callClaimTimedOutGroup = callClaimTimedOutGroup;
    this.callUnlockGroup = callUnlockGroup;
    this.callDeleteMessage = callDeleteMessage;
    
    this.groupStreamKey = groupStreamKey;
    this.groupSetKey = groupSetKey;
    this.consumerGroupId = consumerGroupId;

    this._createPriorityMessageQueueKey = createPriorityMessageQueueKey;
  
    this.isRunning = false;
  }

  async start (): Promise<void> {
    this.isRunning = true;

    this.redis = await clone_redis_connection(this.parentRedis);

    const redisClientId: string = await this.redis.client('ID') as string;
    this.consumerId = `consumer-${redisClientId}`;

    this._poll();
  }

  stop (): Promise<void> {
    if (!this.isRunning) return Promise.resolve();

    return new Promise(resolve => {
      this.isRunning = false;

      super.once('stopped', (): void => {
        this.redis.disconnect(false);
        
        resolve();
      });
    });
  }

  private async _poll (): Promise<void> {
    if (!this.isRunning) {
      super.emit('stopped');
      return;
    }

    const lock = await this._lock();

    if (lock) {
      for (let batchIndex = 0; batchIndex < this.batchSize && this.isRunning; ++batchIndex) {
        const msgData = await this._peek(lock);

        if (msgData) {
          try {
            await this._process(lock, msgData);

            await this._delete(lock, msgData);
          } catch (e) {
            console.error('Failed processing message: ', e);

            break;
          }
        } else {
          break;
        }
      }

      await this._unlock(lock);
    }

    this._poll();
  }

  private async _process (lock: LockHandle, msgData: any) {
    await this.handleMessage(msgData, lock);
  }

  private async _lock (): Promise<LockHandle | null> {
    const claimed = await this._claim_timed_out_lock();

    if (claimed) {
      return claimed;
    }

    const response = await this.redis.xreadgroup(
      'GROUP', this.consumerGroupId, this.consumerId,
      'COUNT', 1,
      'BLOCK', this.pollingTimeoutMs,
      'STREAMS', this.groupStreamKey, '>');

    if (!response) {
      return null;
    }

    const [ streams ] = response as [ any ];

    if (streams) {
      const [ _, messages ] = streams;
      
      if (messages) {
        const [ msg ] = messages;

        const [ msgId, [ _ /*msgContentKey*/, groupId ] ] = msg;

        return { groupId, messageId: msgId, queueId: this._createPriorityMessageQueueKey(groupId), consumerId: this.consumerId };
      }
    }

    return null;
  }

  private async _claim_timed_out_lock (): Promise<LockHandle | null> {
    const [ msgId, groupId ] = await this.callClaimTimedOutGroup(
      [ this.consumerGroupId, this.consumerId, this.groupVisibilityTimeoutMs.toString() ],
      [ this.groupStreamKey ]
    );

    if (msgId) {
      return { groupId, messageId: msgId, queueId: this._createPriorityMessageQueueKey(groupId), consumerId: this.consumerId };
    } else {
      return null;
    }
  }

  private async _unlock (lock: LockHandle): Promise<number> {
    const [ _ /*unlockedCount*/, remainingMessagesCount ] = await this.callUnlockGroup(
      [ this.consumerGroupId, this.consumerId, lock.messageId, lock.groupId ],
      [ this.groupStreamKey, this.groupSetKey, lock.queueId ]
    );

    return remainingMessagesCount;
  }

  private async _peek (lock: LockHandle): Promise<string | null> {
    const [ msgData ] = await this.redis.zrange(lock.queueId, 0, 0) as [ string ];

    return msgData;
  }

  private async _delete (lock: LockHandle, messageData: string): Promise<number> {
    const [ _ /*removedMsgsCount*/, messageQueueLength ] = await this.callDeleteMessage(
      [ lock.groupId, messageData ],
      [ this.groupSetKey, lock.queueId ]);

    return messageQueueLength; //[ removedMsgsCount, messageQueueLength ];
  }
};
