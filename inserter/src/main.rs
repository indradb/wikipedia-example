use std::net::ToSocketAddrs;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufRead, Stdout, Seek, SeekFrom};
use std::collections::HashMap;

use indradb_proto::service;
use capnp::Error as CapnpError;
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
const REQUEST_BUFFER_SIZE: u32 = 10_000;

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

async fn insert_articles(client: &service::Client, f: &File, progress: &mut ProgressBar<Stdout>) -> Result<HashMap<String, Uuid>, Box<dyn Error>> {
    let mut uuids = HashMap::<String, Uuid>::new();
    
    let mut params = Params::new();
    let hasher = params.hash_length(16);

    let mut req = client.bulk_insert_request();
    let mut req_items = req.get().init_items(REQUEST_BUFFER_SIZE);
    let mut req_index = 0u32;

    for line in BufReader::new(f).lines() {
        progress.inc();

        let mut line = line?;
        if line.starts_with("\t") {
            line = line[1..].to_string();
        }

        if uuids.contains_key(&line) {
            continue;
        }

        let uuid = Uuid::from_slice(hasher.hash(line.as_bytes()).as_bytes())?;

        uuids.insert(line.clone(), uuid);

        {
            let req_item = req_items.reborrow().get(req_index);
            let mut builder = req_item.init_vertex().get_vertex()?;
            builder.set_id(uuid.as_bytes());
            builder.set_t("article");
        }
        {
            let req_item = req_items.reborrow().get(req_index + 1);
            let mut builder = req_item.init_vertex_property();
            builder.set_id(uuid.as_bytes());
            builder.set_name("name");
            builder.set_value(&JsonValue::String(line).to_string());
        }

        req_index += 2;

        if req_index >= REQUEST_BUFFER_SIZE {
            let res = req.send().promise.await?;
            res.get()?;
            req = client.bulk_insert_request();
            req_items = req.get().init_items(REQUEST_BUFFER_SIZE);
            req_index = 0;
        }
    }

    if req_index > 0 {
        let res = req.send().promise.await?;
        res.get()?;
    }

    Ok(uuids)
}

async fn insert_links(client: &service::Client, f: &File, uuids: HashMap<String, Uuid>, progress: &mut ProgressBar<Stdout>) -> Result<(), Box<dyn Error>> {
    let mut src_uuid: Option<Uuid> = None;
    let mut req = client.bulk_insert_request();
    let mut req_items = req.get().init_items(REQUEST_BUFFER_SIZE);
    let mut req_index = 0u32;

    for line in BufReader::new(f).lines() {
        progress.inc();

        let line = line?;
        if line.starts_with("\t") {
            let dst_uuid = uuids[&line[1..]];

            let req_item = req_items.reborrow().get(req_index);
            let mut builder = req_item.init_edge().get_key()?;
            builder.set_outbound_id(src_uuid.unwrap().as_bytes());
            builder.set_t("link");
            builder.set_inbound_id(dst_uuid.as_bytes());

            req_index += 1;

            if req_index >= REQUEST_BUFFER_SIZE {
                let res = req.send().promise.await?;
                res.get()?;
                req = client.bulk_insert_request();
                req_items = req.get().init_items(REQUEST_BUFFER_SIZE);
                req_index = 0;
            }
        } else {
            src_uuid = Some(uuids[&line]);
        }
    }

    if req_index > 0 {
        let res = req.send().promise.await?;
        res.get()?;
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut exec = LocalPool::new();
    let spawner = exec.spawner();

    let client = exec.run_until(build_client(&spawner))?;
    
    let mut f = File::open("../data/links.txt")?;
    let line_count = BufReader::new(&f).lines().count();

    f.seek(SeekFrom::Start(0))?;
    let mut article_progress = ProgressBar::new(line_count as u64);
    article_progress.message("inserting articles: ");
    let uuids = exec.run_until(insert_articles(&client, &f, &mut article_progress))?;
    article_progress.finish();

    f.seek(SeekFrom::Start(0))?;
    let mut link_progress = ProgressBar::new(line_count as u64);
    link_progress.message("inserting links: ");
    exec.run_until(insert_links(&client, &f, uuids, &mut link_progress))?;
    link_progress.finish();

    Ok(())
}
