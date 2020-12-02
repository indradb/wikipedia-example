#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;
#[macro_use] extern crate lazy_static;

mod crawler;
mod explorer;
mod util;

use std::error::Error;

use clap::{Arg, App, SubCommand};
use rocket_contrib::templates::Template;
use tokio::task;

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("IndraDB wikipedia example")
        .about("demonstrates IndraDB with the wikipedia dataset")
        .subcommand(SubCommand::with_name("crawl")
            .about("inserts content from the streaming output of parse_archive.py")
            .arg(Arg::with_name("ARCHIVE_INPUT")
                .help("Sets the input archive file to use")
                .required(true)
                .index(1))
            .arg(Arg::with_name("ARCHIVE_DUMP")
                .help("Sets the path of the archive cache dump")
                .required(true)
                .index(2)))
        .subcommand(SubCommand::with_name("explore")
            .about("runs the explorer"))
        .get_matches();

    let mut server = util::Server::start()?;

    let job_result: Result<(), Box<dyn Error>> = task::LocalSet::new().run_until(async move {
        if let Some(matches) = matches.subcommand_matches("crawl") {
            let client = util::retrying_client().await?;
            let article_map = crawler::load_article_map(
                matches.value_of("ARCHIVE_INPUT").unwrap(),
                matches.value_of("ARCHIVE_DUMP").unwrap(),
            ).await?;
            crawler::insert_articles(&client, &article_map).await?;
            crawler::insert_links(&client, &article_map).await?;
        } else if let Some(_) = matches.subcommand_matches("explore") {
            rocket::ignite()
                .attach(Template::fairing())
                .mount("/", routes![explorer::index, explorer::article]).launch();
        } else {
            panic!("unknown command");
        };
        Ok(())
    }).await;

    let stop_result = server.stop();
    job_result?;
    stop_result?;
    Ok(())
}
