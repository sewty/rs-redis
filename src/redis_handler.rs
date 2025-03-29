use redis::{Client, Commands, Connection, RedisResult};
use pyo3::{prelude::*, types::{PyBytes, PyDict}};
use serde::{Serialize, Deserialize};
use rayon::prelude::*;
use anyhow::{anyhow, Result};
use std::sync::{Arc, Mutex};
use polars::prelude::LazyFrame;

// Define the sequence format for efficient storage
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Sequence {
    index: i32,
    sequence: LazyFrame,
    anomaly: i32,
    saliency: LazyFrame,
    failure: i32,
    unseen: bool,
}

struct RedisHandler {
    client: Client,
    connection: Mutex<Connection>,
}

impl RedisHandler {
    fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let connection = Mutex::new(client.get_connection()?);
        let base_key = "sequences";

        Ok(Self { client, connection })
    }

    fn push_sequence(&self, key_suffix: &str, sequence: &Sequence) -> Result<()> {
        let mut conn = self.connection.lock().map_err(|_| anyhow!("Lock error"))?;

        let key = format!("{}_{}", base_key, key_suffix);
        let encoded = bincode::serialize(sequence)?;

        // push to Redis list
        redis::cmd("RPUSH")
            .arg(&key)
            .arg(&encoded)
            .execute(&mut conn);

        Ok(())
    }

    // get length of key
    fn get_length(&self, key_suffix: &str) -> Result<usize> {
        let mut conn = self.connection.lock().map_err(|_| anyhow!("Lock error"))?;
        let key = format!("{}_{}", base_key, key_suffix);

        let len: isize = redis::cmd("LLEN").arg(&key).query(&mut *conn)?;

        Ok(len)
    }

    // get a superbatch of size (batch_size * 4)
    fn get_superbatch(&self, key_suffix: &str, start: isize, count: isize) -> Result<Vec<Sequence>> {
        let mut conn = self.connection.lock().map_err(|_| anyhow!("Lock error"))?;
        let key = format!("{}_{}", base_key, key_suffix);
        
        // use LRANGE to get a superbatch (bsz * 4)
        let encoded_seqs: Vec<Vec<u8>> = redis::cmd("LRANGE")
            .arg(&key)
            .arg(start)
            .arg(start + count - 1)
            .query(&mut *conn)?;

        let sequences: Vec<Sequence> = encoded_seqs.par_iter().filter_map(|bytes| bincode::deserialize(bytes).ok()).collect();
        


    }
}