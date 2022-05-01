import Redis from 'ioredis';

export type RedisArgumentType = string | number;
export type RedisScriptCall = (args: RedisArgumentType[], keys: string[]) => Promise<any>;

export async function prepare_redis_script(redis: Redis, script: string): Promise<RedisScriptCall> {
  const hash: string = await redis.call('SCRIPT', [ 'LOAD', script ]) as string;

  return async (args: RedisArgumentType[], keys: string[]): Promise<any> => {
    return await redis.call('EVALSHA', [ hash, keys.length, ...keys, ...args ]);
  }; 
};

export async function clone_redis_connection(redis: Redis): Promise<Redis> {
  return await new Promise<Redis>((resolve, reject) => {
    const copy: Redis = new Redis(redis.options);

    copy.once('connect', () => { resolve(copy); });
    copy.once('error', (err) => { reject(err); });
  });
}
