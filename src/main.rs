mod crawler;
mod explorer;
mod proc;

use std::error::Error;
use std::fs::File;

use futures::executor::LocalPool;
use clap::{Arg, App, SubCommand};

fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("IndraDB wikipedia example")
        .about("demonstrates IndraDB with the wikipedia dataset")
        .subcommand(SubCommand::with_name("crawl")
            .about("inserts content from the streaming output of parse_archive.py")
            .arg(Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .index(1)))
        .get_matches();

    let mut exec = LocalPool::new();
    let spawner = exec.spawner();

    let mut server = proc::Server::start()?;
    let client = exec.run_until(proc::retrying_client(&spawner))?;

    if let Some(matches) = matches.subcommand_matches("crawl") {
        let f = File::open(matches.value_of("INPUT").unwrap())?;
        let article_map = exec.run_until(crawler::read_archive(f))?;
        exec.run_until(crawler::insert_articles(&client, &article_map))?;
        exec.run_until(crawler::insert_links(&client, &article_map))?;
    }

    server.stop()?;
    Ok(())
}
