use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufRead, Stdout};
use std::collections::{HashMap, VecDeque};

use indradb_proto::service;
use capnp::Error as CapnpError;
use capnp::capability::{Promise, Response};
use serde_json::value::Value as JsonValue;
use uuid::Uuid;
use blake2b_simd::Params;
use pbr::ProgressBar;

const REQUEST_BUFFER_SIZE: usize = 10_000;
const PROMISE_BUFFER_SIZE: usize = 10;

struct BulkInserter<'a> {
    client: &'a service::Client,
    buf: Vec<indradb::BulkInsertItem>,
    promises: VecDeque<Promise<Response<service::bulk_insert_results::Owned>, CapnpError>>
}

impl<'a> BulkInserter<'a> {
    fn new(client: &'a service::Client) -> Self {
        Self {
            client,
            buf: Vec::with_capacity(REQUEST_BUFFER_SIZE),
            promises: VecDeque::with_capacity(PROMISE_BUFFER_SIZE)
        }
    }

    async fn flush(self) -> Result<(), CapnpError> {
        for promise in self.promises {
            let res = promise.await?;
            res.get()?;
        }

        Ok(())
    }

    async fn push(&mut self, item: indradb::BulkInsertItem) -> Result<(), CapnpError> {
        self.buf.push(item);

        if self.buf.len() >= REQUEST_BUFFER_SIZE {
            while self.promises.len() >= PROMISE_BUFFER_SIZE {
                let promise = self.promises.pop_front().unwrap();
                let res = promise.await?;
                res.get()?;
            }

            let mut req = self.client.bulk_insert_request();
            indradb_proto::util::from_bulk_insert_items(
                &self.buf,
                req.get().init_items(self.buf.len() as u32)
            )?;
            self.promises.push_back(req.send().promise);
            self.buf = Vec::with_capacity(REQUEST_BUFFER_SIZE);
        }
        
        Ok(())
    }
}

pub async fn insert_articles(client: &service::Client, f: &File, progress: &mut ProgressBar<Stdout>) -> Result<HashMap<String, Uuid>, Box<dyn Error>> {
    let mut uuids = HashMap::<String, Uuid>::new();
    let mut params = Params::new();
    let hasher = params.hash_length(16);
    let mut inserter = BulkInserter::new(client);
    let article_type = indradb::Type::new("article").unwrap();

    for line in BufReader::new(f).lines() {
        let mut line = line?;
        if line.starts_with('\t') {
            line = line[1..].to_string();
        }

        if !uuids.contains_key(&line) {
            let uuid = Uuid::from_slice(hasher.hash(line.as_bytes()).as_bytes())?;
            uuids.insert(line.clone(), uuid);
            inserter.push(indradb::BulkInsertItem::Vertex(indradb::Vertex::with_id(uuid, article_type.clone()))).await?;
            inserter.push(indradb::BulkInsertItem::VertexProperty(uuid, "name".to_string(), JsonValue::String(line))).await?;
        }

        progress.inc();
    }

    inserter.flush().await?;
    Ok(uuids)
}

pub async fn insert_links(client: &service::Client, f: &File, uuids: HashMap<String, Uuid>, progress: &mut ProgressBar<Stdout>) -> Result<(), Box<dyn Error>> {
    let mut src_uuid: Option<Uuid> = None;
    let mut inserter = BulkInserter::new(client);
    let link_type = indradb::Type::new("link").unwrap();

    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.starts_with('\t') {
            inserter.push(indradb::BulkInsertItem::Edge(indradb::EdgeKey::new(
                src_uuid.unwrap(),
                link_type.clone(),
                uuids[&line[1..]]
            ))).await?;
        } else {
            src_uuid = Some(uuids[&line]);
        }

        progress.inc();
    }

    inserter.flush().await?;
    Ok(())
}
