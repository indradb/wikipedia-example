use std::error::Error as StdError;
use std::mem::replace;

use super::util;

use failure::Fail;
use indradb_proto as proto;
use pbr::ProgressBar;
use serde_json::value::Value as JsonValue;
use tokio::task::JoinHandle;

const REQUEST_BUFFER_SIZE: usize = 10_000;

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
                    client.bulk_insert(buf.into_iter()).await.unwrap();
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

async fn insert_articles(client: proto::Client, article_map: &util::ArticleMap) -> Result<(), proto::ClientError> {
    let mut progress = ProgressBar::new(article_map.article_len());
    progress.message("indexing articles: ");

    let mut inserter = BulkInserter::new(client);
    let article_type = indradb::Type::new("article").unwrap();

    for (article_name, article_uuid) in &article_map.uuids {
        inserter
            .push(indradb::BulkInsertItem::Vertex(indradb::Vertex::with_id(
                *article_uuid,
                article_type.clone(),
            )))
            .await;
        inserter
            .push(indradb::BulkInsertItem::VertexProperty(
                *article_uuid,
                "name".to_string(),
                JsonValue::String(article_name.clone()),
            ))
            .await;
        progress.inc();
    }

    inserter.flush().await;
    progress.finish();
    println!();
    Ok(())
}

async fn insert_links(client: proto::Client, article_map: &util::ArticleMap) -> Result<(), proto::ClientError> {
    let mut progress = ProgressBar::new(article_map.link_len());
    progress.message("indexing links: ");

    let mut inserter = BulkInserter::new(client);
    let link_type = indradb::Type::new("link").unwrap();

    for (src_uuid, dst_uuids) in &article_map.links {
        for dst_uuid in dst_uuids {
            inserter
                .push(indradb::BulkInsertItem::Edge(indradb::EdgeKey::new(
                    *src_uuid,
                    link_type.clone(),
                    *dst_uuid,
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

pub async fn run(client: proto::Client, article_map: util::ArticleMap) -> Result<(), Box<dyn StdError>> {
    insert_articles(client.clone(), &article_map)
        .await
        .map_err(|err| err.compat())?;
    insert_links(client, &article_map).await.map_err(|err| err.compat())?;
    Ok(())
}
