use crate::redis_db::{self, Error};

const QUEUE_LIST: &str = "QUEUE_LIST";
const USER: &str = "USER";
const QUEUE: &str = "QUEUE";
const CURRENT_QUEUE: &str = "CURRENT_QUEUE";
const EVENT: &str = "EVENT";
const ACCESS: &str = "ACCESS";

pub fn total_access(pool: &redis_db::R2D2Pool) -> isize {
    return redis_db::redis_scard(pool, USER).unwrap_or(0);
}

pub fn enqueue(pool: &redis_db::R2D2Pool, queue_id: String, max_access: i32) -> Result<(), Error> {
    if (total_access(pool) as i32) < max_access && length(pool) == 0 {
        let _ = grant_access(pool, queue_id)?;
        Ok(())
    } else {
        let nomor_antrian = redis_db::redis_inc(pool, QUEUE)?;
        let _ = redis_db::redis_add_queue_2(pool, QUEUE_LIST, queue_id.clone())?;
        let _ = redis_db::redis_set(pool, queue_id.clone().as_str(), nomor_antrian.to_string())?;
        Ok(())
    }
}

pub fn dequeue(pool: &redis_db::R2D2Pool, max_access: i32) -> Result<(), Error> {
    if (total_access(pool) as i32) < max_access {
        let queue_id = redis_db::redis_rpop(pool, QUEUE_LIST);
        if queue_id.is_ok() {
            let queue_id = queue_id.unwrap();
            let _ = grant_access(pool, queue_id.clone())?;
            let current_queue_new =
                redis_db::redis_get_global::<i32>(pool, queue_id.clone().as_str()).unwrap_or(0);
            let current_queue = redis_db::redis_get_global::<i32>(pool, CURRENT_QUEUE).unwrap_or(0);
            if current_queue_new > current_queue {
                let _ = redis_db::redis_set(pool, CURRENT_QUEUE, current_queue_new.to_string())?;
            }
        }
    }
    Ok(())
}

pub fn nomor_antrian(pool: &redis_db::R2D2Pool, queue_id: String) -> i32 {
    return redis_db::redis_get_global::<i32>(pool, &queue_id).unwrap_or(-1);
}

pub fn antrian_sekarang(pool: &redis_db::R2D2Pool) -> i32 {
    return redis_db::redis_get_global::<i32>(pool, CURRENT_QUEUE).unwrap_or(0);
}

pub fn length(pool: &redis_db::R2D2Pool) -> isize {
    return redis_db::redis_llen(pool, QUEUE_LIST).unwrap_or(0);
}

pub fn check_access(pool: &redis_db::R2D2Pool, queue_id: String) -> bool {
    return redis_db::redis_sismember(pool, ACCESS, &queue_id).unwrap_or(0) == 1;
}

pub fn check_user(pool: &redis_db::R2D2Pool, queue_id: String) -> bool {
    return redis_db::redis_sismember(pool, USER, &queue_id).unwrap_or(0) == 1;
}


pub fn grant_access(pool: &redis_db::R2D2Pool, queue_id: String) -> Result<isize, Error> {
    let result = redis_db::redis_sadd(pool, USER, &queue_id)?;
    let _ = redis_db::redis_sadd(pool, ACCESS, &queue_id)?;
    Ok(result)
}

pub fn release_access(pool: &redis_db::R2D2Pool, queue_id: String) -> Result<isize, Error> {
    let result = redis_db::redis_srem(pool, ACCESS, &queue_id)?;
    Ok(result)
}

pub fn release_user(pool: &redis_db::R2D2Pool, queue_id: String) -> Result<isize, Error> {
    let result = redis_db::redis_srem(pool, USER, &queue_id)?;
    Ok(result)
}

pub fn release_all_user(pool: &redis_db::R2D2Pool) -> Result<i32, Error> {
    let result = redis_db::redis_del(pool, USER)?;
    Ok(result)
}

pub fn reset(pool: &redis_db::R2D2Pool) {
    let _ = redis_db::redis_del(pool, QUEUE_LIST);
    let _ = redis_db::redis_del(pool, USER);
    let _ = redis_db::redis_del(pool, QUEUE);
    let _ = redis_db::redis_del(pool, CURRENT_QUEUE);
}

pub fn check_event(pool: &redis_db::R2D2Pool, event_id: String) -> bool {
    return redis_db::redis_sismember(pool, EVENT, &event_id).unwrap_or(0) == 1;
}
