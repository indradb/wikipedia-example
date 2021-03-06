use std::collections::{BTreeMap, BTreeSet};

use blake2b_simd::Params;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

lazy_static! {
    static ref HASHER_PARAMS: Params = {
        let mut params = Params::new();
        params.hash_length(16);
        params
    };
}

#[derive(Serialize, Deserialize)]
pub struct ArticleMap {
    pub uuids: BTreeMap<String, Uuid>,
    pub links: BTreeMap<Uuid, BTreeSet<Uuid>>,
}

impl Default for ArticleMap {
    fn default() -> Self {
        Self {
            uuids: BTreeMap::default(),
            links: BTreeMap::default(),
        }
    }
}

impl ArticleMap {
    pub fn insert_article(&mut self, name: &str) -> Uuid {
        if let Some(&uuid) = self.uuids.get(name) {
            return uuid;
        }

        let uuid = article_uuid(name);
        self.uuids.insert(name.to_string(), uuid);
        uuid
    }

    pub fn insert_link(&mut self, src_uuid: Uuid, dst_uuid: Uuid) {
        let container = self.links.entry(src_uuid).or_insert_with(BTreeSet::default);
        container.insert(dst_uuid);
    }

    pub fn article_len(&self) -> u64 {
        self.uuids.len() as u64
    }

    pub fn link_len(&self) -> u64 {
        self.links.iter().map(|(_, v)| v.len()).sum::<usize>() as u64
    }
}

pub fn article_uuid<T: AsRef<[u8]>>(name: T) -> Uuid {
    let hash = HASHER_PARAMS.hash(name.as_ref());
    Uuid::from_slice(hash.as_bytes()).unwrap()
}
