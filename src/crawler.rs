use std::error::Error;
use std::fs::File;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{stdout, Write};

use indradb_proto::service;
use capnp::Error as CapnpError;
use capnp::capability::{Promise, Response};
use serde_json::value::Value as JsonValue;
use uuid::Uuid;
use blake2b_simd::Params;
use pbr::ProgressBar;
use bzip2::read::BzDecoder;
use xml::reader::{EventReader, XmlEvent};
use regex::Regex;

const REQUEST_BUFFER_SIZE: usize = 10_000;
const PROMISE_BUFFER_SIZE: usize = 10;

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

struct BulkInserter<'a> {
    client: &'a service::Client,
    buf: Vec<indradb::BulkInsertItem>,
    promises: VecDeque<Promise<Response<service::bulk_insert_results::Owned>, CapnpError>>
}

impl<'a> BulkInserter<'a> {
    fn new(client: &'a service::Client) -> Self {
        Self {
            client,
            buf: Vec::with_capacity(REQUEST_BUFFER_SIZE),
            promises: VecDeque::with_capacity(PROMISE_BUFFER_SIZE)
        }
    }

    async fn flush(self) -> Result<(), CapnpError> {
        for promise in self.promises {
            let res = promise.await?;
            res.get()?;
        }

        Ok(())
    }

    async fn push(&mut self, item: indradb::BulkInsertItem) -> Result<(), CapnpError> {
        self.buf.push(item);

        if self.buf.len() >= REQUEST_BUFFER_SIZE {
            while self.promises.len() >= PROMISE_BUFFER_SIZE {
                let promise = self.promises.pop_front().unwrap();
                let res = promise.await?;
                res.get()?;
            }

            let mut req = self.client.bulk_insert_request();
            indradb_proto::util::from_bulk_insert_items(
                &self.buf,
                req.get().init_items(self.buf.len() as u32)
            )?;
            self.promises.push_back(req.send().promise);
            self.buf = Vec::with_capacity(REQUEST_BUFFER_SIZE);
        }
        
        Ok(())
    }
}

pub struct ArticleMap {
    uuids_to_names: HashMap<Uuid, String>,
    names_to_uuids: HashMap<String, Uuid>,
    links: HashMap<Uuid, HashSet<Uuid>>,
    hasher: blake2b_simd::Params
}

impl Default for ArticleMap {
    fn default() -> Self {
        let mut params = Params::new();
        params.hash_length(16);

        Self {
            uuids_to_names: HashMap::default(),
            names_to_uuids: HashMap::default(),
            links: HashMap::default(),
            hasher: params,
        }
    }
}

impl ArticleMap {
    pub fn len(&self) -> usize {
        self.uuids_to_names.len()
    }

    fn insert_article(&mut self, name: &str) -> Uuid {
        if let Some(&uuid) = self.names_to_uuids.get(name) {
            return uuid;
        }

        let hash = self.hasher.hash(name.as_bytes());
        let uuid = Uuid::from_slice(hash.as_bytes()).unwrap();
        self.uuids_to_names.insert(uuid, name.to_string());
        self.names_to_uuids.insert(name.to_string(), uuid);
        uuid
    }

    fn insert_link(&mut self, src_uuid: Uuid, dst_uuid: Uuid) {
        let container = self.links.entry(src_uuid).or_insert_with(|| HashSet::default());
        container.insert(dst_uuid);
    }
}

enum ArchiveReadState {
    Ignore,
    Page,
    Title,
    Text,
}

pub async fn read_archive(f: File) -> Result<ArticleMap, Box<dyn Error>> {
    let mut article_map = ArticleMap::default();
    
    let decompressor = BzDecoder::new(f);
    let parser = EventReader::new(decompressor);

    let mut src: String = String::new();
    let mut content: String = String::new();
    let mut state = ArchiveReadState::Ignore;

    let mut processed = 0usize;

    let wiki_link_re = Regex::new(r"\[\[([^\[\]|]+)(|[\]]+)?\]\]").unwrap();

    print!("reading archive: 0");
    stdout().flush()?;

    for event in parser {
        let event = event?;

        match state {
            ArchiveReadState::Ignore => {
                match event {
                    XmlEvent::StartElement { name, .. } if name.local_name == "page" => {
                        src = String::new();
                        content = String::new();
                        state = ArchiveReadState::Page;
                    },
                    _ => {}
                }
            },
            ArchiveReadState::Page => {
                match event {
                    XmlEvent::StartElement { name, .. } if name.local_name == "title" => {
                        state = ArchiveReadState::Title;
                    },
                    XmlEvent::StartElement { name, .. } if name.local_name == "text" => {
                        state = ArchiveReadState::Text;
                    },
                    XmlEvent::EndElement { name } if name.local_name == "page" => {
                        content = content.trim().to_string();
                        debug_assert!(src.len() > 0);
                        debug_assert!(content.len() > 0);

                        let blacklisted = ARTICLE_NAME_PREFIX_BLACKLIST.iter().any(|prefix| {
                            src.starts_with(prefix)
                        }) || content.starts_with(REDIRECT_PREFIX);
                        if !blacklisted {
                            let src_uuid = article_map.insert_article(&src);
                            for cap in wiki_link_re.captures_iter(&content) {
                                let dst = &cap[1];
                                let dst_uuid = article_map.insert_article(dst);
                                article_map.insert_link(src_uuid, dst_uuid);
                            }
                        }

                        state = ArchiveReadState::Ignore;
                    },
                    _ => {}
                }
            },
            ArchiveReadState::Title => {
                match event {
                    XmlEvent::Characters(s) => {
                        src.push_str(&s);
                    }
                    XmlEvent::EndElement { name } if name.local_name == "title" => {
                        state = ArchiveReadState::Page;
                    },
                    _ => {}
                }
            },
            ArchiveReadState::Text => {
                match event {
                    XmlEvent::Characters(s) => {
                        content.push_str(&s);
                    },
                    XmlEvent::EndElement { name } if name.local_name == "text" => {
                        state = ArchiveReadState::Page;
                    },
                    _ => {}
                }
            }
        }

        processed += 1;
        if processed % 1000 == 0 {
            print!("\rreading archive: {}", processed);
            stdout().flush()?;
        }
    }

    println!();
    Ok(article_map)
}

pub async fn insert_articles(client: &service::Client, article_map: &ArticleMap) -> Result<(), Box<dyn Error>> {
    let mut progress = ProgressBar::new(article_map.len() as u64);
    progress.message("indexing articles: ");

    let mut inserter = BulkInserter::new(client);
    let article_type = indradb::Type::new("article").unwrap();

    for (article_name, article_uuid) in &article_map.names_to_uuids {
        inserter.push(indradb::BulkInsertItem::Vertex(indradb::Vertex::with_id(*article_uuid, article_type.clone()))).await?;
        inserter.push(indradb::BulkInsertItem::VertexProperty(*article_uuid, "name".to_string(), JsonValue::String(article_name.clone()))).await?;
        progress.inc();
    }

    inserter.flush().await?;
    progress.finish();
    println!();
    Ok(())
}

pub async fn insert_links(client: &service::Client, article_map: &ArticleMap) -> Result<(), Box<dyn Error>> {
    let mut progress = ProgressBar::new(article_map.len() as u64);
    progress.message("indexing links: ");

    let mut inserter = BulkInserter::new(client);
    let link_type = indradb::Type::new("link").unwrap();

    for (src_uuid, dst_uuids) in &article_map.links {
        for dst_uuid in dst_uuids {
            inserter.push(indradb::BulkInsertItem::Edge(indradb::EdgeKey::new(*src_uuid, link_type.clone(), *dst_uuid))).await?;
        }
        progress.inc();
    }

    inserter.flush().await?;
    progress.finish();
    println!();
    Ok(())
}
