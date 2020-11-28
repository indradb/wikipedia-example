use std::net::ToSocketAddrs;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufRead, Stdout, Seek, SeekFrom};
use std::collections::{HashMap, VecDeque};

use indradb_proto::service;
use capnp::Error as CapnpError;
use capnp::capability::{Promise, Response};
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{twoparty, RpcSystem};
use futures::executor::{LocalPool, LocalSpawner};
use futures::prelude::*;
use futures::task::LocalSpawn;
use serde_json::value::Value as JsonValue;
use uuid::Uuid;
use blake2b_simd::Params;
use pbr::ProgressBar;

const PORT: u16 = 27615;
const REQUEST_BUFFER_SIZE: usize = 10_000;
const PROMISE_BUFFER_SIZE: usize = 100;

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

    async fn send(&mut self) -> Result<(), CapnpError> {
        if !self.buf.is_empty() {
            if self.promises.len() >= PROMISE_BUFFER_SIZE {
                for _ in 0..PROMISE_BUFFER_SIZE/10 {
                    let promise = self.promises.pop_front().unwrap();
                    let res = promise.await?;
                    res.get()?;
                }
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

    async fn push(&mut self, item: indradb::BulkInsertItem) -> Result<(), CapnpError> {
        self.buf.push(item);
        if self.buf.len() >= REQUEST_BUFFER_SIZE {
            self.send().await?;
        }
        Ok(())
    }
}

async fn build_client(spawner: &LocalSpawner) -> Result<service::Client, CapnpError> {
    let addr = format!("127.0.0.1:{}", PORT).to_socket_addrs().unwrap().next().unwrap();
    let stream = async_std::net::TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;
    let (reader, writer) = stream.split();

    let rpc_network = Box::new(twoparty::VatNetwork::new(
        reader,
        writer,
        Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let client: service::Client = rpc_system.bootstrap(Side::Server);

    spawner
        .spawn_local_obj(Box::pin(rpc_system.map(|_| ())).into())
        .map_err(|err| CapnpError::failed(format!("spawn failed: {}", err)))?;

    Ok(client)
}

async fn insert(client: &service::Client, f: &File, progress: &mut ProgressBar<Stdout>) -> Result<(), Box<dyn Error>> {
    let mut src_uuid: Option<Uuid> = None;
    let mut uuids = HashMap::<String, Uuid>::new();
    
    let mut params = Params::new();
    let hasher = params.hash_length(16);
    
    let mut inserter = BulkInserter::new(client);

    let article_type = indradb::Type::new("article").unwrap();
    let link_type = indradb::Type::new("link").unwrap();

    for line in BufReader::new(f).lines() {
        let line = line?;
        let (article_name, is_source) = if line.starts_with('\t') {
            (line[1..].to_string(), false)
        } else {
            (line, true)
        };

        if !uuids.contains_key(&article_name) {
            let uuid = Uuid::from_slice(hasher.hash(article_name.as_bytes()).as_bytes())?;
            uuids.insert(article_name.clone(), uuid);
            inserter.push(indradb::BulkInsertItem::Vertex(indradb::Vertex::with_id(uuid, article_type.clone()))).await?;
            inserter.push(indradb::BulkInsertItem::VertexProperty(uuid, "name".to_string(), JsonValue::String(article_name.clone()))).await?;
        }

        if is_source {
            src_uuid = Some(uuids[&article_name]);
        } else {
            inserter.push(indradb::BulkInsertItem::Edge(indradb::EdgeKey::new(
                src_uuid.unwrap(),
                link_type.clone(),
                uuids[&article_name]
            ))).await?;
        }

        progress.inc();
    }

    inserter.flush().await?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut exec = LocalPool::new();
    let spawner = exec.spawner();

    let client = exec.run_until(build_client(&spawner))?;
    
    let mut f = File::open("../data/links.txt")?;
    let line_count = BufReader::new(&f).lines().count() as u64;

    f.seek(SeekFrom::Start(0))?;
    let mut progress = ProgressBar::new(line_count);
    progress.message("indexing content: ");
    exec.run_until(insert(&client, &f, &mut progress))?;
    progress.finish();

    Ok(())
}
