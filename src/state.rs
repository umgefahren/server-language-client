use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    pub(crate) map: Arc<RwLock<HashMap<String, String>>>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
