import Redis from 'ioredis';

export type RedisArgumentType = string | number;
export type RedisScriptCall = (args: RedisArgumentType[], keys: string[]) => Promise<any>;

export async function prepare_redis_script(redis: Redis.Redis, script: string): Promise<RedisScriptCall> {
  const hash: string = await redis.script('LOAD', script) as string;

  return async (args: RedisArgumentType[], keys: string[]): Promise<any> => {
    return await redis.evalsha(hash, keys.length, ...keys, ...args);
  }; 
};

export async function clone_redis_connection(redis: Redis.Redis): Promise<Redis.Redis> {
  return await new Promise<Redis.Redis>((resolve, reject) => {
    const copy = new Redis({ ...redis.options, lazyConnect: false }) as Redis.Redis;

    copy.once('connect', () => { resolve(copy); });
    copy.once('error', (err: any) => { reject(err); });
  });
}
