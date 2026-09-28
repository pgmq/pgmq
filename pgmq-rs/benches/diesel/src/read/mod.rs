use chrono::{DateTime, Utc};
use diesel::deserialize::FromSql;
use diesel::pg::{Pg, PgValue};
use diesel::sql_types::*;
use diesel::{FromSqlRow, QueryableByName};
use serde_derive::Deserialize;

pub mod query_fragment;
pub mod query_source;
pub mod sql_function;

#[derive(Clone, Debug, Deserialize, QueryableByName)]
#[non_exhaustive]
pub struct MessageQueryableByName<T = serde_json::Value, H = serde_json::Value> {
    #[diesel(sql_type = BigInt)]
    pub msg_id: i64,
    #[diesel(sql_type = Integer)]
    pub read_ct: i32,
    #[diesel(sql_type = Timestamptz)]
    pub enqueued_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub last_read_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Timestamptz)]
    pub vt: DateTime<Utc>,
    #[diesel(sql_type = Json)]
    pub message: T,
    #[diesel(sql_type = Nullable<Json>)]
    pub headers: Option<H>,
}

#[derive(Clone, Debug, Deserialize, FromSqlRow)]
#[non_exhaustive]
pub struct MessageFromSqlRow<T = serde_json::Value, H = serde_json::Value> {
    pub msg_id: i64,
    pub read_ct: i32,
    pub enqueued_at: DateTime<Utc>,
    pub last_read_at: Option<DateTime<Utc>>,
    pub vt: DateTime<Utc>,
    pub message: T,
    pub headers: Option<H>,
}

#[derive(SqlType)]
#[diesel(postgres_type(name = "metrics_result", schema = "pgmq"))]
pub struct PgMessage;

type PgMessageTuple = (
    // msg_id
    BigInt,
    // read_ct
    Integer,
    // enqueued_at
    Timestamptz,
    // last_read_at
    Nullable<Timestamptz>,
    // vt
    Timestamptz,
    // message
    Jsonb,
    // headers
    Nullable<Jsonb>,
);

impl<T, H> FromSql<PgMessage, Pg> for MessageFromSqlRow<T, H>
where
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let (msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers): (
            _,
            _,
            _,
            _,
            _,
            serde_json::Value,
            Option<serde_json::Value>,
        ) = FromSql::<Record<PgMessageTuple>, Pg>::from_sql(bytes)?;

        let headers = if let Some(headers) = headers {
            Some(H::deserialize(headers)?)
        } else {
            None
        };

        Ok(Self {
            msg_id,
            read_ct,
            enqueued_at,
            last_read_at,
            vt,
            message: T::deserialize(message)?,
            headers,
        })
    }
}
