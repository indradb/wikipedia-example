#[macro_use] extern crate lazy_static;

use std::error::Error as StdError;
use std::fs::File;
use std::io::{BufReader, Write, stdout};
use std::collections::{HashMap, HashSet};
use std::str;
use std::path::Path;
use std::convert::TryInto;

use failure::Fail;
use indradb_proto as proto;
use serde_json::value::Value as JsonValue;
use uuid::Uuid;
use blake2b_simd::Params;
use pbr::ProgressBar;
use bzip2::bufread::BzDecoder;
use quick_xml::{Reader, events::Event};
use regex::Regex;
use serde::{Serialize, Deserialize};
use tokio::task;
use clap::{App, Arg};
use tonic::transport::Endpoint;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const PORT: u16 = 27615;
const REQUEST_BUFFER_SIZE: usize = 10_000;

const ARTICLE_NAME_PREFIX_BLACKLIST: [&str; 7] = [
    "Wikipedia:",
    "WP:",
    ":",
    "File:",
    "Image:",
    "Template:",
    "User:",
];

const REDIRECT_PREFIX: &str = "#REDIRECT [[";

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

struct BulkInserter {
    requests: mpsc::Sender<indradb::BulkInsertItem>,
    handle: JoinHandle<()>,
}

impl BulkInserter {
    fn new(mut client: proto::Client) -> Self {
        let (tx, mut rx) = mpsc::channel(REQUEST_BUFFER_SIZE);

        let handle = tokio::spawn(async move {
            let mut buf = Vec::with_capacity(REQUEST_BUFFER_SIZE);

            while let Some(item) = rx.recv().await {
                buf.push(item);

                if buf.len() >= REQUEST_BUFFER_SIZE {
                    client.bulk_insert(buf.drain(..)).await.unwrap();
                }
            }

            if !buf.is_empty() {
                client.bulk_insert(buf.drain(..)).await.unwrap();
            }
        });

        Self {
            requests: tx,
            handle,
        }
    }

    async fn flush(self) {
        drop(self.requests);
        self.handle.await.unwrap();
    }

    async fn push(&mut self, item: indradb::BulkInsertItem) {
        self.requests.send(item).await.unwrap();
    }
}

// TODO: investigate memory/speed tradeoff of BTreeMap vs HashMap here
#[derive(Serialize, Deserialize)]
struct ArticleMap {
    uuids: HashMap<String, Uuid>,
    links: HashMap<Uuid, HashSet<Uuid>>
}

impl Default for ArticleMap {
    fn default() -> Self {
        Self {
            uuids: HashMap::default(),
            links: HashMap::default(),
        }
    }
}

impl ArticleMap {
    fn insert_article(&mut self, name: &str) -> Uuid {
        if let Some(&uuid) = self.uuids.get(name) {
            return uuid;
        }

        let uuid = article_uuid(name);
        self.uuids.insert(name.to_string(), uuid);
        uuid
    }

    fn insert_link(&mut self, src_uuid: Uuid, dst_uuid: Uuid) {
        let container = self.links.entry(src_uuid).or_insert_with(HashSet::default);
        container.insert(dst_uuid);
    }
}

enum ArchiveReadState {
    Ignore,
    Page,
    MostRecentRevision,
    Title,
    Text,
}

async fn read_archive(f: File) -> Result<ArticleMap, Box<dyn StdError>> {
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
    let mut last_article_map_len = 0;

    let wiki_link_re = Regex::new(r"\[\[([^\[\]|]+)(|[\]]+)?\]\]").unwrap();

    print!("reading archive: 0");
    stdout().flush()?;

    loop {
        state = match (state, reader.read_event(&mut buf)?) {
            (ArchiveReadState::Ignore, Event::Start(ref e)) if e.name() == page_tag => {
                src = String::new();
                content = String::new();
                ArchiveReadState::Page
            },
            (ArchiveReadState::Page, Event::Start(ref e)) if e.name() == revision_tag => {
                ArchiveReadState::MostRecentRevision
            },
            (ArchiveReadState::Page, Event::Start(ref e)) if e.name() == title_tag => {
                ArchiveReadState::Title
            },
            (ArchiveReadState::MostRecentRevision, Event::Start(ref e)) if e.name() == text_tag => {
                ArchiveReadState::Text
            },
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

                ArchiveReadState::Ignore
            },
            (ArchiveReadState::Title, Event::Text(ref e)) => {
                debug_assert!(src.is_empty());
                src.push_str(str::from_utf8(e)?);

                let blacklisted = ARTICLE_NAME_PREFIX_BLACKLIST.iter().any(|prefix| {
                    src.starts_with(prefix)
                });

                if blacklisted {
                    ArchiveReadState::Ignore
                } else {
                    ArchiveReadState::Title
                }
            },
            (ArchiveReadState::Title, Event::End(ref e)) if e.name() == title_tag => {
                ArchiveReadState::Page
            },
            (ArchiveReadState::Text, Event::Text(ref e)) => {
                debug_assert!(content.is_empty());
                content.push_str(str::from_utf8(e)?);

                let blacklisted = content.starts_with(REDIRECT_PREFIX);

                if blacklisted {
                    ArchiveReadState::Ignore
                } else {
                    ArchiveReadState::Text
                }
            },
            (ArchiveReadState::Text, Event::End(ref e)) if e.name() == text_tag => {
                ArchiveReadState::MostRecentRevision
            },
            (_, Event::Eof) => break,
            (state, _) => state
        };

        buf.clear();

        if article_map.uuids.len() - last_article_map_len >= 1000 {
            last_article_map_len = article_map.uuids.len();
            print!("\rreading archive: {}", last_article_map_len);
            stdout().flush()?;
        }
    }

    println!("\rreading archive: done");

    Ok(article_map)
}

async fn load_article_map(input_filepath: &str, dump_filepath: &str) -> Result<ArticleMap, Box<dyn StdError>> {
    if Path::new(dump_filepath).exists() {
        print!("reading dump...");
        stdout().flush()?;
        let article_map = bincode::deserialize_from(File::open(dump_filepath)?)?;
        println!("\rreading dump: done");
        Ok(article_map)
    } else {
        let article_map = read_archive(File::open(input_filepath)?).await?;
        bincode::serialize_into(File::create(dump_filepath)?, &article_map)?;
        Ok(article_map)
    }
}

async fn insert_articles(client: proto::Client, article_map: &ArticleMap) -> Result<(), proto::ClientError> {
    let mut progress = ProgressBar::new(article_map.uuids.len() as u64);
    progress.message("indexing articles: ");

    let mut inserter = BulkInserter::new(client);
    let article_type = indradb::Type::new("article").unwrap();

    for (article_name, article_uuid) in &article_map.uuids {
        inserter.push(indradb::BulkInsertItem::Vertex(indradb::Vertex::with_id(*article_uuid, article_type.clone()))).await;
        inserter.push(indradb::BulkInsertItem::VertexProperty(*article_uuid, "name".to_string(), JsonValue::String(article_name.clone()))).await;
        progress.inc();
    }

    inserter.flush().await;
    progress.finish();
    println!();
    Ok(())
}

async fn insert_links(client: proto::Client, article_map: &ArticleMap) -> Result<(), proto::ClientError> {
    let mut progress = ProgressBar::new(article_map.uuids.len() as u64);
    progress.message("indexing links: ");

    let mut inserter = BulkInserter::new(client);
    let link_type = indradb::Type::new("link").unwrap();

    for (src_uuid, dst_uuids) in &article_map.links {
        for dst_uuid in dst_uuids {
            inserter.push(indradb::BulkInsertItem::Edge(indradb::EdgeKey::new(*src_uuid, link_type.clone(), *dst_uuid))).await;
        }
        progress.inc();
    }

    inserter.flush().await;
    progress.finish();
    println!();
    Ok(())
}

async fn build_client() -> Result<proto::Client, Box<dyn StdError>> {
    let endpoint: Endpoint = format!("http://127.0.0.1:{}", PORT).try_into().unwrap();
    let client = proto::Client::new(endpoint).await.map_err(|err| err.compat())?;
    Ok(client)
}

#[tokio::main(basic_scheduler)]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let matches = App::new("IndraDB wikipedia example")
        .about("demonstrates IndraDB with the wikipedia dataset")
        .arg(Arg::with_name("ARCHIVE_INPUT")
            .help("Sets the input archive file to use")
            .required(true)
            .index(1))
        .arg(Arg::with_name("ARCHIVE_DUMP")
            .help("Sets the path of the archive cache dump")
            .required(true)
            .index(2))
        .get_matches();

    task::LocalSet::new().run_until(async move {
        let article_map = load_article_map(
            matches.value_of("ARCHIVE_INPUT").unwrap(),
            matches.value_of("ARCHIVE_DUMP").unwrap(),
        ).await?;
        insert_articles(build_client().await?, &article_map).await.map_err(|err| err.compat())?;
        insert_links(build_client().await?, &article_map).await.map_err(|err| err.compat())?;
        Ok(())
    }).await
}
