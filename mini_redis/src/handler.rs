use crate::state::St;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Deserialize, Debug)]
#[serde(tag = "cmd", rename_all = "UPPERCASE")]
pub enum Command {
    Ping,
    Set { key: String, value: String },
    Get { key: String },
    Del { key: String },
    Keys,
    Expire { key: String, seconds: u64 },
    Ttl { key: String },
    Incr { key: String },
    Decr { key: String },
    Save,
}

#[derive(Serialize)]
pub struct ResponseOk {
    pub status: String,
    pub value: Option<String>,
    pub count: Option<usize>,
    pub keys: Option<Vec<String>>,
    pub ttl: Option<i64>,
}

#[derive(Serialize)]
pub struct ResponseOkWithValue {
    pub status: String,
    pub value: i64,
}

#[derive(Serialize)]
pub struct ResponseError {
    pub status: String,
    pub message: String,
}

#[derive(Clone)]
pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

fn is_expired(entry: &Entry) -> bool {
    match entry.expires_at {
        Some(t) => Instant::now() > t,
        None => false,
    }
}

pub async fn process_command(cmd: Command, db: &St) -> String {
    match cmd {
        Command::Ping => {
            let r = ResponseOk {
                status: "ok".to_string(),
                value: None,
                count: None,
                keys: None,
                ttl: None,
            };
            serde_json::to_string(&r).unwrap()
        }
        Command::Set { key, value } => {
            let mut s = db.lock().await;
            s.insert(
                key,
                Entry {
                    value,
                    expires_at: None,
                },
            );

            let r = ResponseOk {
                status: "ok".to_string(),
                value: None,
                count: None,
                keys: None,
                ttl: None,
            };
            serde_json::to_string(&r).unwrap()
        }
        Command::Get { key } => {
            let mut s = db.lock().await;
            let v = if let Some(e) = s.get(&key) {
                if is_expired(e) {
                    s.remove(&key);
                    None
                } else {
                    Some(e.value.clone())
                }
            } else {
                None
            };

            let r = ResponseOk {
                status: "ok".to_string(),
                value: v,
                count: None,
                keys: None,
                ttl: None,
            };
            serde_json::to_string(&r).unwrap()
        }
        Command::Del { key } => {
            let mut s = db.lock().await;
            let c = if s.remove(&key).is_some() { 1 } else { 0 };

            let r = ResponseOk {
                status: "ok".to_string(),
                value: None,
                count: Some(c),
                keys: None,
                ttl: None,
            };
            serde_json::to_string(&r).unwrap()
        }
        Command::Keys => {
            let s = db.lock().await;
            let k: Vec<String> = s
                .iter()
                .filter(|(_, e)| !is_expired(e))
                .map(|(k, _)| k.clone())
                .collect();

            let r = ResponseOk {
                status: "ok".to_string(),
                value: None,
                count: None,
                keys: Some(k),
                ttl: None,
            };
            serde_json::to_string(&r).unwrap()
        }
        Command::Expire { key, seconds } => {
            let mut s = db.lock().await;

            if let Some(e) = s.get_mut(&key) {
                e.expires_at = Some(Instant::now() + std::time::Duration::from_secs(seconds));
                let r = ResponseOk {
                    status: "ok".to_string(),
                    value: None,
                    count: None,
                    keys: None,
                    ttl: None,
                };
                serde_json::to_string(&r).unwrap()
            } else {
                let r = ResponseOk {
                    status: "ok".to_string(),
                    value: None,
                    count: None,
                    keys: None,
                    ttl: None,
                };
                serde_json::to_string(&r).unwrap()
            }
        }
        Command::Ttl { key } => {
            let mut s = db.lock().await;

            if let Some(e) = s.get(&key) {
                if is_expired(e) {
                    s.remove(&key);
                    let r = ResponseOk {
                        status: "ok".to_string(),
                        value: None,
                        count: None,
                        keys: None,
                        ttl: Some(-2),
                    };
                    serde_json::to_string(&r).unwrap()
                } else if let Some(t) = e.expires_at {
                    let ttl_val = t.duration_since(Instant::now()).as_secs() as i64;
                    let r = ResponseOk {
                        status: "ok".to_string(),
                        value: None,
                        count: None,
                        keys: None,
                        ttl: Some(ttl_val),
                    };
                    serde_json::to_string(&r).unwrap()
                } else {
                    let r = ResponseOk {
                        status: "ok".to_string(),
                        value: None,
                        count: None,
                        keys: None,
                        ttl: Some(-1),
                    };
                    serde_json::to_string(&r).unwrap()
                }
            } else {
                let r = ResponseOk {
                    status: "ok".to_string(),
                    value: None,
                    count: None,
                    keys: None,
                    ttl: Some(-2),
                };
                serde_json::to_string(&r).unwrap()
            }
        }
        Command::Incr { key } => {
            let mut s = db.lock().await;
            let nv = if let Some(e) = s.get_mut(&key) {
                if is_expired(e) {
                    s.remove(&key);
                    1
                } else {
                    match e.value.parse::<i64>() {
                        Ok(num) => num + 1,
                        Err(_) => {
                            let err = ResponseError {
                                status: "error".to_string(),
                                message: "value is not an integer".to_string(),
                            };
                            return serde_json::to_string(&err).unwrap();
                        }
                    }
                }
            } else {
                1
            };
            s.insert(
                key,
                Entry {
                    value: nv.to_string(),
                    expires_at: None,
                },
            );

            let r = ResponseOkWithValue {
                status: "ok".to_string(),
                value: nv,
            };

            serde_json::to_string(&r).unwrap()
        }
        Command::Decr { key } => {
            let mut s = db.lock().await;
            let nv = if let Some(e) = s.get_mut(&key) {
                if is_expired(e) {
                    s.remove(&key);
                    -1
                } else {
                    match e.value.parse::<i64>() {
                        Ok(num) => num - 1,
                        Err(_) => {
                            let err = ResponseError {
                                status: "error".to_string(),
                                message: "value is not an integer".to_string(),
                            };
                            return serde_json::to_string(&err).unwrap();
                        }
                    }
                }
            } else {
                -1
            };
            s.insert(
                key,
                Entry {
                    value: nv.to_string(),
                    expires_at: None,
                },
            );

            let r = ResponseOkWithValue {
                status: "ok".to_string(),
                value: nv,
            };
            serde_json::to_string(&r).unwrap()
        }
        Command::Save => {
            let s = db.lock().await;
            let d: Vec<(String, String, Option<u64>)> = s
                .iter()
                .filter_map(|(k, e)| {
                    if is_expired(e) {
                        None
                    } else {
                        let t = e
                            .expires_at
                            .map(|ex| ex.duration_since(Instant::now()).as_secs());
                        Some((k.clone(), e.value.clone(), t))
                    }
                })
                .collect();

            match serde_json::to_string(&d) {
                Ok(j) => {
                    std::fs::write("dump.json", j).unwrap();
                    let r = ResponseOk {
                        status: "ok".to_string(),
                        value: None,
                        count: None,
                        keys: None,
                        ttl: None,
                    };
                    serde_json::to_string(&r).unwrap()
                }
                Err(_) => {
                    let err = ResponseError {
                        status: "error".to_string(),
                        message: "failed to serialize data".to_string(),
                    };
                    serde_json::to_string(&err).unwrap()
                }
            }
        }
    }
}

pub async fn handler(mut socket: TcpStream, db: St) {
    let (rh, mut wh) = socket.split();
    let mut r = BufReader::new(rh);
    let mut l = String::new();

    loop {
        l.clear();
        let b = match r.read_line(&mut l).await {
            Ok(n) => n,
            Err(_) => break,
        };

        if b == 0 {
            break;
        }

        match serde_json::from_str::<Command>(&l) {
            Ok(cmd) => {
                let resp = process_command(cmd, &db).await;
                let _ = wh
                    .write_all(format!("{}\n", resp).as_bytes())
                    .await;
            }
            Err(_) => {
                let err = ResponseError {
                    status: "error".to_string(),
                    message: "invalid json or unknown command".to_string(),
                };
                let es = serde_json::to_string(&err).unwrap();
                let _ = wh
                    .write_all(format!("{}\n", es).as_bytes())
                    .await;
            }
        }
    }
}
