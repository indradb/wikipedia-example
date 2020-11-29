use std::net::ToSocketAddrs;
use std::error::Error;
use std::process::{Command, Child};
use std::time::Duration;

use indradb_proto::service;
use capnp::Error as CapnpError;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{twoparty, RpcSystem};
use futures::executor::LocalSpawner;
use futures::prelude::*;
use futures::task::LocalSpawn;
use async_std::task::sleep;

const PORT: u16 = 27615;
const SECRET: &str = "OME88YorohonzPNWEFsi0dIsouXWqeO$";
const DATABASE_URL: &str = "sled://data/wikipedia.sled";

pub async fn client(spawner: &LocalSpawner) -> Result<service::Client, CapnpError> {
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

    client.ping_request().send().promise.await?;
    Ok(client)
}

pub async fn retrying_client(spawner: &LocalSpawner) -> Result<service::Client, CapnpError> {
    let mut last_err: Option<CapnpError> = None;

    for _ in 0..5 {
        match client(spawner).await {
            Ok(client) => {
                return Ok(client);
            },
            Err(err) => {
                last_err = Some(err);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }

    Err(last_err.unwrap())
}

pub struct Server(Child);

impl Server {
    pub fn start() -> Result<Self, Box<dyn Error>> {
        let child = Command::new("indradb")
            .env("SECRET", SECRET)
            .env("DATABASE_URL", DATABASE_URL)
            .env("RUST_BACKTRACE", "1")
            .env("SLEDDB_COMPRESSION", "true")
            .spawn()?;

        Ok(Self { 0: child })
    }

    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        unsafe {
            libc::kill(self.0.id() as i32, libc::SIGTERM);
        }
        self.0.wait()?;
        Ok(())
    }
}
