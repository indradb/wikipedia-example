use std::fs::File;
use std::io::{BufReader, Write, stdout};
use std::error::Error as StdError;
use std::str;

use super::util::ArticleMap;

use bzip2::bufread::BzDecoder;
use quick_xml::{Reader, events::Event};
use regex::Regex;

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

pub fn write_dump(archive_path: &str, dump_path: &str) -> Result<(), Box<dyn StdError>> {
    let article_map = read_archive(File::open(archive_path)?)?;
    bincode::serialize_into(File::create(dump_path)?, &article_map)?;
    Ok(())
}

pub fn read_dump(dump_path: &str) -> Result<ArticleMap, Box<dyn StdError>> {
    print!("reading dump...");
    stdout().flush()?;
    let article_map = bincode::deserialize_from(File::open(dump_path)?)?;
    println!("\rreading dump: done");
    Ok(article_map)
}
