import {createClient} from 'redis';
import ioRedis from 'ioredis';
const redis_password = 'rjsemfwlak';
const redis_host = '127.0.0.1';
const redis_port = 6379;
const redis_url = `redis://:${redis_password}@${redis_host}:${redis_port}`;
const redis_database = Number(process.env.database??'0');

/*
let redisClient = createClient({
    url: redis_url
});
redisClient.connect()
    .then(() => console.log("redis connect success"))
    .catch(console.error);

const reconnect = async () => {
    await redisClient.disconnect();
    redisClient = createClient({
        url: redis_url
    });
    redisClient.connect()
        .then(() => console.log("redis connect success"))
        .catch(console.error);
}
*/

const ioRedisClient = new ioRedis({
    port: redis_port,
    host: redis_host,
    password: redis_password,
    db: redis_database
})

const ioLockClient =  new ioRedis({
    port: redis_port,
    host: redis_host,
    password: redis_password,
    db: redis_database
})

//export {redisClient, reconnect, ioRedisClient}
export {ioRedisClient, ioLockClient}