use std::net::ToSocketAddrs;
use std::error::Error;
use std::process::{Command, Child};
use std::time::Duration;

use indradb_proto::service;
use capnp::Error as CapnpError;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{twoparty, RpcSystem};
use blake2b_simd::Params;
use uuid::Uuid;
use tokio::task;
use tokio::time::sleep;
use tokio_util::compat::Tokio02AsyncReadCompatExt;
use futures::AsyncReadExt;
use futures::FutureExt;

const PORT: u16 = 27615;
const SECRET: &str = "OME88YorohonzPNWEFsi0dIsouXWqeO$";
const DATABASE_URL: &str = "sled://data/wikipedia.sled";

lazy_static! {
    static ref HASHER_PARAMS: Params = {
        let mut params = Params::new();
        params.hash_length(16);
        params
    };
}

pub async fn client() -> Result<service::Client, CapnpError> {
    let addr = format!("127.0.0.1:{}", PORT).to_socket_addrs().unwrap().next().unwrap();
    let stream = tokio::net::TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;
    let (reader, writer) = Tokio02AsyncReadCompatExt::compat(stream).split();

    let rpc_network = Box::new(twoparty::VatNetwork::new(
        reader,
        writer,
        Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let client: service::Client = rpc_system.bootstrap(Side::Server);

    task::spawn_local(Box::pin(rpc_system.map(|_| ())));

    client.ping_request().send().promise.await?;

    Ok(client)
}

pub async fn retrying_client() -> Result<service::Client, CapnpError> {
    let mut last_err: Option<CapnpError> = None;

    for _ in 0..5 {
        match client().await {
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

pub fn article_uuid<T: AsRef<[u8]>>(name: T) -> Uuid {
    let hash = HASHER_PARAMS.hash(name.as_ref());
    Uuid::from_slice(hash.as_bytes()).unwrap()
}
