#[macro_use] extern crate lazy_static;

use std::error::Error;
use std::process::{Command, Child};
use std::convert::TryInto;

use indradb_proto as proto;
use blake2b_simd::Params;
use uuid::Uuid;
use tonic::transport::Endpoint;

const PORT: u16 = 27615;

lazy_static! {
    static ref HASHER_PARAMS: Params = {
        let mut params = Params::new();
        params.hash_length(16);
        params
    };
}

pub fn article_uuid<T: AsRef<[u8]>>(name: T) -> Uuid {
    let hash = HASHER_PARAMS.hash(name.as_ref());
    Uuid::from_slice(hash.as_bytes()).unwrap()
}

pub async fn client() -> Result<proto::Client, proto::ClientError> {
    let endpoint: Endpoint = format!("http://127.0.0.1:{}", PORT).try_into().unwrap();
    proto::Client::new(endpoint).await
}

pub struct Server(Child);

impl Server {
    pub fn start(database_path: &str) -> Result<Self, Box<dyn Error>> {
        let child = Command::new("indradb")
            .args(&["rocksdb", database_path, "--compression", "true"])
            .env("RUST_BACKTRACE", "1")
            .spawn()?;

        Ok(Self { 0: child })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.0.id() as i32, libc::SIGTERM);
        }
        self.0.wait().unwrap();
    }
}
