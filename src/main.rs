#[macro_use] extern crate clap;
#[macro_use] extern crate lazy_static;

mod util;
mod parser;
mod indexer;
mod explorer;

use std::error::Error as StdError;
use clap::{App, SubCommand, Arg};
use std::process::{Command, Child};

pub struct Server(Child);

// TODO: suppress stdout
// TODO: make port dynamic
impl Server {
    pub fn start(database_path: &str) -> Result<Self, Box<dyn StdError>> {
        let child = Command::new("indradb")
            .args(&["rocksdb", database_path])
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

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let archive_arg = Arg::with_name("ARCHIVE_PATH")
        .help("path to the wikipedia dataset archive")
        .long("archive-path")
        .value_name("ARCHIVE_PATH")
        .required(true)
        .takes_value(true);

    let archive_dump_arg = Arg::with_name("DUMP_PATH")
        .help("path to the archive dump, an intermediate representation for faster re-indexing")
        .long("dump-path")
        .value_name("DUMP_PATH")
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
        .subcommand(SubCommand::with_name("parse").arg(&archive_arg).arg(&archive_dump_arg))
        .subcommand(SubCommand::with_name("index").arg(&archive_dump_arg).arg(&datastore_arg))
        .subcommand(SubCommand::with_name("explore").arg(&datastore_arg).arg(&port_arg))
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("parse") {
        let archive_path = matches.value_of("ARCHIVE_PATH").unwrap();
        let archive_dump_path = matches.value_of("DUMP_PATH").unwrap();
        parser::write_dump(archive_path, archive_dump_path)
    } else if let Some(matches) = matches.subcommand_matches("index") {
        let archive_dump_path = matches.value_of("DUMP_PATH").unwrap();
        let database_path = matches.value_of("DATABASE_PATH").unwrap();
        let _server = Server::start(database_path)?;
        let article_map = parser::read_dump(archive_dump_path)?;
        indexer::run(article_map).await
    } else if let Some(matches) = matches.subcommand_matches("explore") {
        let database_path = matches.value_of("DATABASE_PATH").unwrap();
        let port = value_t!(matches.value_of("PORT"), u16).unwrap_or_else(|err| err.exit());
        let _server = Server::start(database_path)?;
        explorer::run(port).await
    } else {
        panic!("no subcommand specified");
    }
}
