use std::sync::Arc;
use scylla::Session;
use crate::AnyResult;
use scylla::IntoTypedRows;
use scylla::frame::value::Counter;

pub async fn inc(key: &str, session: Arc<Session>) -> AnyResult<usize> {
    let prepared = session
    .prepare("update ks.moflip_counter set next_id = next_id + 1 where id_counter = ?")
    .await.unwrap();

    let qresult = session.execute(&prepared, (key,))
    .await
    .unwrap();

    Ok(qresult.rows_num().unwrap_or(0))
}

pub async fn reset_inc(key: &str, session: Arc<Session>) -> AnyResult<usize> {
    let prepared = session
    .prepare("update ks.moflip_counter set next_id = 0 where id_counter = ?")
    .await.unwrap();

    let qresult = session.execute(&prepared, (key,))
    .await
    .unwrap();

    Ok(qresult.rows_num().unwrap_or(0))
}

pub async fn dec(key: &str, session: Arc<Session>) -> AnyResult<usize> {
    let prepared = session
    .prepare("update ks.moflip_counter set next_id = next_id - 1 where id_counter = ?")
    .await.unwrap();

    let qresult = session.execute(&prepared, (key,))
    .await
    .unwrap();

    Ok(qresult.rows_num().unwrap_or(0))
}

pub async fn current_index(key: &str, session: Arc<Session>) -> AnyResult<i64> {
    let rows = session.query("select next_id from ks.moflip_counter where id_counter = ?", (key,)).await.unwrap().rows;
    match rows {
        Some(res) => {
            for row in res.into_typed::<(Counter, )>() {
                let (counter_value, ): (Counter, ) = row.unwrap();
                let counter_int_value = counter_value.0;
                return Ok(counter_int_value);
            }
        }
        _ => ()
    } 
    Ok(0)
}