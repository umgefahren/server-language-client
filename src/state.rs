use chashmap::CHashMap;

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    pub(crate) map: CHashMap<String, String>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            map: CHashMap::new()
        }
    }
}
