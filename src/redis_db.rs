#![allow(unused_imports)]
use ::redis::FromRedisValue;
use r2d2_redis::{
    r2d2,
    redis::{self, Commands},
    RedisConnectionManager,
};
use std::time::Duration;
use thiserror::Error;
type Result<T> = std::result::Result<T, Error>;

pub type R2D2Pool = r2d2::Pool<RedisConnectionManager>;
pub type R2D2Con = r2d2::PooledConnection<RedisConnectionManager>;

const CACHE_POOL_MAX_OPEN: u32 = 16;
const CACHE_POOL_MIN_IDLE: u32 = 8;
const CACHE_POOL_TIMEOUT_SECONDS: u64 = 1;
const CACHE_POOL_EXPIRE_SECONDS: u64 = 60;

pub fn connect(host: String) -> Result<R2D2Pool> {
    let manager = RedisConnectionManager::new(host).unwrap();
    let pool = r2d2::Pool::builder()
        .max_size(CACHE_POOL_MAX_OPEN)
        .max_lifetime(Some(Duration::from_secs(CACHE_POOL_EXPIRE_SECONDS)))
        .min_idle(Some(CACHE_POOL_MIN_IDLE))
        .build(manager)
        .unwrap();
    Ok(pool)
}

pub(in crate::redis_db) fn get_con(pool: &R2D2Pool) -> Result<R2D2Con> {
    let con = pool
        .get_timeout(Duration::from_secs(CACHE_POOL_TIMEOUT_SECONDS))
        .unwrap();
    Ok(con)
}

pub fn redis_keys(pool: &R2D2Pool, key: &str) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("KEYS")
        .arg(key)
        .query::<Vec<String>>(&mut *conn)
        .unwrap();
    Ok(result)
}

pub fn redis_keys_with_prefix(pool: &R2D2Pool, key: &str) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("KEYS")
        .arg(key)
        .query::<Vec<String>>(&mut *conn)
        .unwrap();
    Ok(result)
}

pub fn redis_keys_2(pool: &R2D2Pool, key: &str) -> Result<Vec<String>> {
    let mut result = Vec::new();
    let mut conn = get_con(&pool).unwrap();
    let iter = redis::cmd("SCAN")
        .arg(0)
        .arg("MATCH")
        .arg(key)
        .arg("COUNT")
        .arg(10000000)
        .cursor_arg(0)
        .clone()
        .iter(&mut *conn)
        .unwrap();

    for x in iter {
        result.push(x);
    }
    Ok(result)
}

pub fn redis_sort(pool: &R2D2Pool, key: &str) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("SORT")
        .arg(key)
        .query::<Vec<String>>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_get(pool: &R2D2Pool, key: &str) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("GET")
        .arg(key)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_verify_queue(pool: &R2D2Pool, key: i32) -> Result<String> {
    let mut conn = get_con(&pool)?;
    // let script: &'static str = r#"
    //     local current_value = tonumber(redis.call('GET', 'counter'))

    //     if current_value ~= nil and current_value < {} then
    //         redis.call('INCR', 'counter')
    //     end

    //     return redis.call('GET', 'counter')
    // "#;

    let result = redis::cmd("EVAL")
        .arg(format!(
            r#"
        local current_value = tonumber(redis.call('GET', 'counter'))
        
        if current_value ~= nil and current_value < {} then
            redis.call('INCR', 'counter')
        end
        
        return redis.call('GET', 'counter')
    "#,
            key
        ))
        .arg(0)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_get_global<T: r2d2_redis::redis::FromRedisValue>(
    pool: &R2D2Pool,
    key: &str,
) -> Result<T> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("GET")
        .arg(key)
        .query::<T>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_get_full(pool: &R2D2Pool, key: &str) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("GET")
        .arg(key)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_set(pool: &R2D2Pool, key: &str, value: String) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("SET")
        .arg(key)
        .arg(&value)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_set_ttl(pool: &R2D2Pool, key: &str, value: String, ttl: usize) -> Result<()> {
    let mut con = get_con(&pool).unwrap();
    con.set(key, value).map_err(R2D2Error::RedisCMDError)?;
    if ttl > 0 {
        con.expire(key, ttl).map_err(R2D2Error::RedisCMDError)?;
    }

    Ok(())
}

pub fn redis_increment(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("INCR")
        .arg(key)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;

    Ok(result)
}

pub fn redis_zadd(pool: &R2D2Pool, key: &str, score: &str, value: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;

    let result = redis::cmd("ZADD")
        .arg(key)
        .arg(score)
        .arg(value)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_zrem(pool: &R2D2Pool, key: &str, value: &str) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("ZREM")
        .arg(key)
        .arg(value)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_zremrange(pool: &R2D2Pool, key: &str, min: &str, max: &str) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("ZREMRANGEBYSCORE")
        .arg(key)
        .arg(min)
        .arg(max)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_zrangebyscore(
    pool: &R2D2Pool,
    key: &str,
    min: &str,
    max: &str,
) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("ZRANGEBYSCORE")
        .arg(key)
        .arg(min)
        .arg(max)
        .query::<Vec<String>>(&mut *conn)
        .unwrap();
    Ok(result)
}

pub fn redis_zrevrange(
    pool: &R2D2Pool,
    key: &str,
    min: &str,
    max: &str,
) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("ZREVRANGE")
        .arg(key)
        .arg(min)
        .arg(max)
        .query::<Vec<String>>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_zrange(
    pool: &R2D2Pool,
    key: &str,
    min: &str,
    max: &str,
) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("ZRANGE")
        .arg(key)
        .arg(min)
        .arg(max)
        .query::<Vec<String>>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_zcount(pool: &R2D2Pool, key: &str, min: &str, max: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("ZCOUNT")
        .arg(key)
        .arg(min)
        .arg(max)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_set_full(pool: &R2D2Pool, key: &str, value: String) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("SET")
        .arg(key)
        .arg(&value)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}
pub fn redis_exists(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("EXISTS")
        .arg(key)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    // println!("result: {}", result);
    Ok(result)
}

pub fn redis_exists_with_prefix(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("EXISTS")
        .arg(key)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    // println!("result: {}", result);
    Ok(result)
}
pub fn redis_inc(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("INCR")
        .arg(key)
        .query::<i32>(&mut *conn)
        .unwrap();
    // println!("result: {}", result);
    Ok(result)
}

pub fn redis_decr_access_queue(pool: &R2D2Pool, key1: &str, key2: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("EVAL")
        .arg(format!(
            r#"
        local current_access = tonumber(redis.call('GET', KEYS[1]))
        local current_queue = tonumber(redis.call('GET', KEYS[2]))
        
        if current_access ~= nil and current_access > 0 then
            redis.call('DECR', KEYS[1])
        end

        if current_queue ~= nil and current_queue > 0 then
            redis.call('DECR', KEYS[2])
        end
        
        return redis.call('GET', KEYS[1])
    "#,
        ))
        .arg(2)
        .arg(key1)
        .arg(key2)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;

    Ok(result)
}

pub fn redis_reset_stat(pool: &R2D2Pool, key1: &str, key2: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("EVAL")
        .arg(format!(
            r#"
        local current_access = tonumber(redis.call('GET', KEYS[1]))
        local current_queue = tonumber(redis.call('GET', KEYS[2]))
        
        redis.call('SET', KEYS[1], 0)
      
        redis.call('SET', KEYS[2], 0)
   
        
        return redis.call('GET', KEYS[1])
    "#,
        ))
        .arg(2)
        .arg(key1)
        .arg(key2)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;

    Ok(result)
}

pub fn redis_decr(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("DECR")
        .arg(key)
        .query::<i32>(&mut *conn)
        .unwrap();
    // println!("result: {}", result);
    Ok(result)
}
pub fn redis_add_queue(pool: &R2D2Pool, key: &str, value: String) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("LPUSH")
        .arg(key)
        .arg(&value)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_add_queue_2(pool: &R2D2Pool, key: &str, value: String) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("LPUSH")
        .arg(key)
        .arg(&value)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}
pub fn redis_add_list(pool: &R2D2Pool, key: &str, value: String) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("RPUSH")
        .arg(key)
        .arg(&value)
        .query::<String>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}
pub fn redis_add_list_2(pool: &R2D2Pool, key: &str, value: String) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("RPUSH")
        .arg(key)
        .arg(&value)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_add_list_with_expire_2(
    pool: &R2D2Pool,
    key: &str,
    value: String,
    expire: usize,
) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("RPUSH")
        .arg(key)
        .arg(&value)
        .query::<i32>(&mut *conn)
        .map_err(R2D2Error::RedisCMDError)?;

    redis::cmd("EXPIRE").arg(key).arg(expire);

    Ok(result)
}

pub fn redis_llen(pool: &R2D2Pool, key: &str) -> Result<isize> {
    let mut conn = get_con(&pool)?;

    let len: isize = conn.llen(key).map_err(R2D2Error::RedisCMDError)?;
    Ok(len)
}
pub fn redis_lrange(pool: &R2D2Pool, key: &str) -> Result<Vec<String>> {
    let mut conn = get_con(&pool)?;
    let range = redis_llen(&pool, key)?;
    let result = conn
        .lrange(key, 0, range - 1)
        .map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}
pub fn redis_del(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("DEL").arg(key).query::<i32>(&mut *conn).unwrap();
    Ok(result)
}

pub fn redis_del_full(pool: &R2D2Pool, key: &str) -> Result<i32> {
    let mut conn = get_con(&pool)?;
    let result = redis::cmd("DEL").arg(key).query::<i32>(&mut *conn).unwrap();
    // println!("result: {}", result);
    Ok(result)
}

pub fn redis_lpop(pool: &R2D2Pool, key: &str) -> Result<String> {
    let mut conn = get_con(&pool)?;
    let result = conn.lpop(key).map_err(R2D2Error::RedisCMDError)?;
    Ok(result)
}

pub fn redis_rpop(pool: &R2D2Pool, key: &str) -> redis::RedisResult<String> {
    let mut conn = get_con(&pool).unwrap();
    let result = conn.rpop::<&str, String>(key);
    result
}

pub fn redis_lset(
    pool: &R2D2Pool,
    key: &str,
    value: String,
    idx: isize,
) -> redis::RedisResult<String> {
    let mut conn = get_con(&pool).unwrap();
    let result = conn.lset(key, idx, value);
    result
}

pub fn redis_scard(pool: &R2D2Pool, key: &str) -> Result<isize> {
    let mut conn = get_con(&pool)?;
    let len: isize = conn.scard(key).map_err(R2D2Error::RedisCMDError)?;
    Ok(len)
}

pub fn redis_sadd(pool: &R2D2Pool, key: &str, value: &str) -> Result<isize> {
    let mut conn = get_con(&pool)?;
    let len: isize = conn.sadd(key, value).map_err(R2D2Error::RedisCMDError)?;
    Ok(len)
}

pub fn redis_srem(pool: &R2D2Pool, key: &str, value: &str) -> Result<isize> {
    let mut conn = get_con(&pool)?;
    let len: isize = conn.srem(key, value).map_err(R2D2Error::RedisCMDError)?;
    Ok(len)
}

pub fn redis_sismember(pool: &R2D2Pool, key: &str, value: &str) -> Result<isize> {
    let mut conn = get_con(&pool)?;
    let len: isize = conn.sismember(key, value).map_err(R2D2Error::RedisCMDError)?;
    Ok(len)
}

#[derive(Error, Debug)]
pub enum R2D2Error {
    #[error("could not get redis connection from pool : {0}")]
    RedisPoolError(r2d2_redis::r2d2::Error),
    #[error("error parsing string from redis result: {0}")]
    RedisTypeError(r2d2_redis::redis::RedisError),
    #[error("error executing redis command: {0}")]
    RedisCMDError(r2d2_redis::redis::RedisError),
    #[error("error creating Redis client: {0}")]
    RedisClientError(r2d2_redis::redis::RedisError),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("r2d2 error: {0}")]
    R2D2Error(#[from] R2D2Error),
}
