mod crawler;
mod explorer;
mod proc;

use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufRead, Seek, SeekFrom};

use futures::executor::LocalPool;
use pbr::ProgressBar;

fn main() -> Result<(), Box<dyn Error>> {
    let mut exec = LocalPool::new();
    let spawner = exec.spawner();

    proc::Server::start()?;
    let client = exec.run_until(proc::retrying_client(&spawner))?;

    let mut f = File::open("../data/links.txt")?;
    let line_count = BufReader::new(&f).lines().count() as u64;

    f.seek(SeekFrom::Start(0))?;
    let mut article_progress = ProgressBar::new(line_count);
    article_progress.message("indexing articles: ");
    let uuids = exec.run_until(crawler::insert_articles(&client, &f, &mut article_progress))?;
    article_progress.finish();
    println!();

    f.seek(SeekFrom::Start(0))?;
    let mut link_progress = ProgressBar::new(line_count);
    link_progress.message("indexing links: ");
    exec.run_until(crawler::insert_links(&client, &f, uuids, &mut link_progress))?;
    link_progress.finish();
    println!();

    Ok(())
}
