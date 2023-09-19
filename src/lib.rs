pub mod redis_db;
pub mod queue;

pub mod scheduler {
    use crate::redis_db::R2D2Pool;
    use crate::queue;
    
    pub fn run(pool: &R2D2Pool, max_access: i32) {
        let _ = queue::release_all_user(pool);
    
        for _ in 0..max_access {
            let _ = queue::dequeue(pool, max_access);
        }
    }
}