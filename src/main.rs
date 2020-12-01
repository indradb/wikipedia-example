#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;
#[macro_use] extern crate lazy_static;

mod crawler;
mod explorer;
mod util;

use std::error::Error;
use std::fs::File;

use clap::{Arg, App, SubCommand};
use rocket_contrib::templates::Template;
use tokio::task;
use indradb_proto::service;
use capnp::Error as CapnpError;

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("IndraDB wikipedia example")
        .about("demonstrates IndraDB with the wikipedia dataset")
        .subcommand(SubCommand::with_name("crawl")
            .about("inserts content from the streaming output of parse_archive.py")
            .arg(Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("explore")
            .about("runs the explorer"))
        .get_matches();

    let mut server = util::Server::start()?;

    if let Some(matches) = matches.subcommand_matches("crawl") {
        let client = task::LocalSet::new().run_until(util::retrying_client()).await?;
        let f = File::open(matches.value_of("INPUT").unwrap())?;
        let article_map = crawler::read_archive(f).await?;
        crawler::insert_articles(&client, &article_map).await?;
        crawler::insert_links(&client, &article_map).await?;
    } else if let Some(_) = matches.subcommand_matches("explore") {
        rocket::ignite()
            .attach(Template::fairing())
            .mount("/", routes![explorer::index, explorer::article]).launch();
    }

    server.stop()?;
    Ok(())
}
