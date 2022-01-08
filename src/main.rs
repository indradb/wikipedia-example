#[macro_use]
extern crate clap;
#[macro_use]
extern crate lazy_static;

mod explorer;
mod indexer;

use std::convert::TryInto;
use std::error::Error as StdError;
use std::ffi::OsStr;
use std::process::{Child, Command};

use clap::{App, Arg, SubCommand};
use tokio::time::{sleep, Duration};

pub struct Server {
    child: Child,
}

impl Server {
    pub fn start(database_path: &OsStr) -> Result<Self, Box<dyn StdError>> {
        let child = Command::new("indradb/target/release/indradb-server")
            .args(&[
                OsStr::new("rocksdb"),
                database_path,
            ])
            .env("RUST_BACKTRACE", "1")
            .spawn()?;
        Ok(Server { child })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        self.child.wait().unwrap();
    }
}

async fn get_client() -> Result<indradb_proto::Client, indradb_proto::ClientError> {
    let mut client = indradb_proto::Client::new("grpc://127.0.0.1:27615".try_into().unwrap()).await?;
    client.ping().await?;
    Ok(client)
}

async fn get_client_retrying() -> Result<indradb_proto::Client, indradb_proto::ClientError> {
    let mut retry_count = 10u8;
    let mut last_err = Option::<indradb_proto::ClientError>::None;

    while retry_count > 0 {
        match get_client().await {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_err = Some(err);
                if retry_count == 0 {
                    break;
                } else {
                    sleep(Duration::from_secs(1)).await;
                    retry_count -= 1;
                }
            }
        }
    }

    Err(last_err.unwrap())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let archive_arg = Arg::with_name("ARCHIVE_PATH")
        .help("path to the wikipedia dataset archive")
        .long("archive-path")
        .value_name("ARCHIVE_PATH")
        .required(true)
        .takes_value(true);

    let port_arg = Arg::with_name("PORT")
        .help("port to run the webserver on")
        .long("port")
        .value_name("PORT")
        .default_value("8080")
        .takes_value(true);

    let matches = App::new("IndraDB wikipedia example")
        .about("demonstrates IndraDB with the wikipedia dataset")
        .arg(
            Arg::with_name("DATABASE_PATH")
                .help("path for storing the IndraDB results")
                .long("database-path")
                .value_name("DATABASE_PATH")
                .required(true)
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("index").arg(&archive_arg))
        .subcommand(SubCommand::with_name("explore").arg(&port_arg))
        .get_matches();

    let database_path = matches.value_of_os("DATABASE_PATH").unwrap();
    let _server = Server::start(database_path)?;
    let client = get_client_retrying().await.unwrap();

    if let Some(matches) = matches.subcommand_matches("index") {
        let archive_path = matches.value_of_os("ARCHIVE_PATH").unwrap();
        indexer::run(client, archive_path).await
    } else if let Some(matches) = matches.subcommand_matches("explore") {
        let port = value_t!(matches.value_of("PORT"), u16).unwrap_or_else(|err| err.exit());
        explorer::run(client, port).await
    } else {
        panic!("no subcommand specified");
    }
}
