#[macro_use]
extern crate clap;

mod explorer;
mod indexer;

use std::convert::TryInto;
use std::error::Error as StdError;
use std::ffi::OsStr;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};

use clap::{App, Arg, SubCommand};
use indradb_proto as proto;
use tonic::transport::Endpoint;

pub struct Server {
    child: Child,
    address: String,
}

impl Server {
    pub fn start(database_path: &OsStr) -> Result<Self, Box<dyn StdError>> {
        let mut child = Command::new("indradb/target/release/indradb-server")
            .args(&[
                OsStr::new("--address"),
                OsStr::new("127.0.0.1:0"),
                OsStr::new("rocksdb"),
                database_path,
            ])
            .env("RUST_BACKTRACE", "1")
            .stdout(Stdio::piped())
            .spawn()?;

        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let address = lines.next().unwrap()?;
        Ok(Server { child, address })
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

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let archive_arg = Arg::with_name("ARCHIVE_PATH")
        .help("path to the wikipedia dataset archive")
        .long("archive-path")
        .value_name("ARCHIVE_PATH")
        .required(true)
        .takes_value(true);

    let datastore_arg = Arg::with_name("DATABASE_PATH")
        .help("path for storing the IndraDB results")
        .long("database-path")
        .value_name("DATABASE_PATH")
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
        .subcommand(SubCommand::with_name("index").arg(&archive_arg).arg(&datastore_arg))
        .subcommand(SubCommand::with_name("explore").arg(&datastore_arg).arg(&port_arg))
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("index") {
        let archive_path = matches.value_of_os("ARCHIVE_PATH").unwrap();
        let database_path = matches.value_of_os("DATABASE_PATH").unwrap();
        let server = Server::start(database_path)?;
        let endpoint: Endpoint = server.address.clone().try_into()?;
        let client = proto::Client::new(endpoint).await?;
        indexer::run(client, archive_path).await
    } else if let Some(matches) = matches.subcommand_matches("explore") {
        let database_path = matches.value_of_os("DATABASE_PATH").unwrap();
        let port = value_t!(matches.value_of("PORT"), u16).unwrap_or_else(|err| err.exit());
        let server = Server::start(database_path)?;
        let endpoint: Endpoint = server.address.clone().try_into()?;
        let client = proto::Client::new(endpoint).await?;
        explorer::run(client, port).await
    } else {
        panic!("no subcommand specified");
    }
}
