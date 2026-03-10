use crate::handler::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type St = Arc<Mutex<HashMap<String, Entry>>>;
