use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{stdout, BufReader, Write};
use std::mem::replace;
use std::str;
use std::time::Instant;

use bzip2::bufread::BzDecoder;
use indradb_proto as proto;
use pbr::ProgressBar;
use quick_xml::{events::Event, Reader};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use uuid::Uuid;

const REQUEST_BUFFER_SIZE: usize = 10_000;

const ARTICLE_NAME_PREFIX_BLACKLIST: [&str; 7] = ["Wikipedia:", "WP:", ":", "File:", "Image:", "Template:", "User:"];

const REDIRECT_PREFIX: &str = "#REDIRECT [[";

#[derive(Default, Serialize, Deserialize)]
pub struct ArticleMap {
    pub uuids: HashMap<String, Uuid>,
    pub links: HashMap<Uuid, HashSet<Uuid>>,
}

impl ArticleMap {
    pub fn insert_article(&mut self, name: &str) -> Uuid {
        if let Some(&uuid) = self.uuids.get(name) {
            return uuid;
        }
        let uuid = indradb::util::generate_uuid_v1();
        self.uuids.insert(name.to_string(), uuid);
        uuid
    }

    pub fn insert_link(&mut self, src_uuid: Uuid, dst_uuid: Uuid) {
        let container = self.links.entry(src_uuid).or_insert_with(HashSet::default);
        container.insert(dst_uuid);
    }

    pub fn article_len(&self) -> u64 {
        self.uuids.len() as u64
    }

    pub fn link_len(&self) -> u64 {
        self.links.values().map(|v| v.len()).sum::<usize>() as u64
    }
}

enum ArchiveReadState {
    Ignore,
    Page,
    MostRecentRevision,
    Title,
    Text,
}

fn read_archive(f: File) -> Result<ArticleMap, Box<dyn StdError>> {
    let mut article_map = ArticleMap::default();

    let mut buf = Vec::new();
    let f = BufReader::new(f);
    let decompressor = BufReader::new(BzDecoder::new(f));
    let mut reader = Reader::from_reader(decompressor);
    reader.trim_text(true);
    reader.check_end_names(false);

    let mut src: String = String::new();
    let mut content: String = String::new();
    let mut state = ArchiveReadState::Ignore;

    let page_tag = "page".as_bytes();
    let title_tag = "title".as_bytes();
    let text_tag = "text".as_bytes();
    let revision_tag = "revision".as_bytes();

    let mut progress_start = Instant::now();
    let mut last_total_read_count = 0u32;
    let mut total_read_count = 0u32;

    let wiki_link_re = Regex::new(r"\[\[([^\[\]|]+)(|[\]]+)?\]\]").unwrap();

    print!("reading archive");
    stdout().flush()?;

    loop {
        state = match (state, reader.read_event(&mut buf)?) {
            (ArchiveReadState::Ignore, Event::Start(ref e)) if e.name() == page_tag => {
                src = String::new();
                content = String::new();
                ArchiveReadState::Page
            }
            (ArchiveReadState::Page, Event::Start(ref e)) if e.name() == revision_tag => {
                ArchiveReadState::MostRecentRevision
            }
            (ArchiveReadState::Page, Event::Start(ref e)) if e.name() == title_tag => ArchiveReadState::Title,
            (ArchiveReadState::MostRecentRevision, Event::Start(ref e)) if e.name() == text_tag => {
                ArchiveReadState::Text
            }
            (ArchiveReadState::MostRecentRevision, Event::End(ref e)) if e.name() == revision_tag => {
                content = content.trim().to_string();
                debug_assert!(!src.is_empty());
                debug_assert!(!content.is_empty());

                let src_uuid = article_map.insert_article(&src);
                for cap in wiki_link_re.captures_iter(&content) {
                    let dst = &cap[1];
                    let dst_uuid = article_map.insert_article(dst);
                    article_map.insert_link(src_uuid, dst_uuid);
                }

                total_read_count += 1;

                let elapsed = progress_start.elapsed();
                if elapsed.as_secs() >= 1 {
                    let read_speed_str = (total_read_count - last_total_read_count).to_string();
                    print!(
                        "\rreading archive: {} articles ({}/s)",
                        total_read_count, read_speed_str
                    );
                    for _ in 0..(10i16 - read_speed_str.len() as i16) {
                        print!(" ");
                    }
                    stdout().flush()?;
                    progress_start = Instant::now();
                    last_total_read_count = total_read_count;
                }

                ArchiveReadState::Ignore
            }
            (ArchiveReadState::Title, Event::Text(ref e)) => {
                debug_assert!(src.is_empty());
                src.push_str(str::from_utf8(e)?);

                let blacklisted = ARTICLE_NAME_PREFIX_BLACKLIST
                    .iter()
                    .any(|prefix| src.starts_with(prefix));

                if blacklisted {
                    ArchiveReadState::Ignore
                } else {
                    ArchiveReadState::Title
                }
            }
            (ArchiveReadState::Title, Event::End(ref e)) if e.name() == title_tag => ArchiveReadState::Page,
            (ArchiveReadState::Text, Event::Text(ref e)) => {
                debug_assert!(content.is_empty());
                content.push_str(str::from_utf8(e)?);

                let blacklisted = content.starts_with(REDIRECT_PREFIX);

                if blacklisted {
                    ArchiveReadState::Ignore
                } else {
                    ArchiveReadState::Text
                }
            }
            (ArchiveReadState::Text, Event::End(ref e)) if e.name() == text_tag => ArchiveReadState::MostRecentRevision,
            (_, Event::Eof) => break,
            (state, _) => state,
        };

        buf.clear();
    }

    println!();
    Ok(article_map)
}

struct BulkInserter {
    requests: async_channel::Sender<Vec<indradb::BulkInsertItem>>,
    workers: Vec<JoinHandle<()>>,
    buf: Vec<indradb::BulkInsertItem>,
}

impl BulkInserter {
    fn new(client: proto::Client) -> Self {
        let (tx, rx) = async_channel::bounded::<Vec<indradb::BulkInsertItem>>(10);
        let mut workers = Vec::default();

        for _ in 0..10 {
            let rx = rx.clone();
            let mut client = client.clone();
            workers.push(tokio::spawn(async move {
                while let Ok(buf) = rx.recv().await {
                    client.bulk_insert(buf).await.unwrap();
                }
            }));
        }

        Self {
            requests: tx,
            workers,
            buf: Vec::with_capacity(REQUEST_BUFFER_SIZE),
        }
    }

    async fn flush(self) {
        if !self.buf.is_empty() {
            self.requests.send(self.buf).await.unwrap();
        }
        self.requests.close();
        for worker in self.workers.into_iter() {
            worker.await.unwrap();
        }
    }

    async fn push(&mut self, item: indradb::BulkInsertItem) {
        self.buf.push(item);
        if self.buf.len() >= REQUEST_BUFFER_SIZE {
            let buf = replace(&mut self.buf, Vec::with_capacity(REQUEST_BUFFER_SIZE));
            self.requests.send(buf).await.unwrap();
        }
    }
}

async fn insert_articles(client: proto::Client, article_map: &ArticleMap) -> Result<(), Box<dyn StdError>> {
    let mut progress = ProgressBar::new(article_map.article_len());
    progress.message("indexing articles: ");

    let mut inserter = BulkInserter::new(client);
    let article_type = indradb::Identifier::new("article").unwrap();

    for (article_name, article_uuid) in &article_map.uuids {
        inserter
            .push(indradb::BulkInsertItem::Vertex(indradb::Vertex::with_id(
                *article_uuid,
                article_type,
            )))
            .await;
        inserter
            .push(indradb::BulkInsertItem::VertexProperty(
                *article_uuid,
                indradb::Identifier::new("name")?,
                indradb::Json::new(serde_json::Value::String(article_name.clone())),
            ))
            .await;
        progress.inc();
    }

    inserter.flush().await;
    progress.finish();
    println!();
    Ok(())
}

async fn insert_links(client: proto::Client, article_map: &ArticleMap) -> Result<(), proto::ClientError> {
    let mut progress = ProgressBar::new(article_map.link_len());
    progress.message("indexing links: ");

    let mut inserter = BulkInserter::new(client);
    let link_type = indradb::Identifier::new("link").unwrap();

    for (src_uuid, dst_uuids) in &article_map.links {
        for dst_uuid in dst_uuids {
            inserter
                .push(indradb::BulkInsertItem::Edge(indradb::Edge::new(
                    *src_uuid, link_type, *dst_uuid,
                )))
                .await;
        }
        progress.add(dst_uuids.len() as u64);
    }

    inserter.flush().await;
    progress.finish();
    println!();
    Ok(())
}

pub async fn run(mut client: proto::Client, archive_path: &OsStr) -> Result<(), Box<dyn StdError>> {
    let start_time = Instant::now();
    client.index_property(indradb::Identifier::new("name")?).await?;
    let article_map = read_archive(File::open(archive_path)?)?;
    insert_articles(client.clone(), &article_map).await?;
    insert_links(client, &article_map).await?;
    println!("finished in {} seconds", start_time.elapsed().as_secs());
    Ok(())
}
