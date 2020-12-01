use crate::util;

use indradb_proto::service;
use rocket_contrib::templates::Template;
use rocket::http::RawStr;
use serde::Serialize;

#[derive(Serialize)]
struct IndexArguments;

#[get("/")]
pub fn index() -> Template {
    Template::render("index", IndexArguments)
}

// def get_article(self, article_name):
//     # Get the ID of the article we want from its name
//     article_id = article_uuid(article_name)

//     # Get all of the data we want from IndraDB in a single
//     # request/transaction
//     vertex_query = indradb.SpecificVertexQuery(article_id)
//     trans = self.client.transaction()

//     vertex_data, edge_count, edge_data = capnp.join_promises([
//         trans.get_vertices(vertex_query),
//         trans.get_edge_count(article_id, None, "outbound"),
//         trans.get_edges(vertex_query.outbound(EDGE_LIMIT).t("link")),
//     ]).wait()

//     if len(vertex_data) == 0:
//         raise HTTPError(404)

//     name_data = trans.get_vertex_properties(indradb.SpecificEdgeQuery(*[e.key for e in edge_data]).inbound(EDGE_LIMIT).property("name")).wait()
    
//     inbound_edge_ids = [e.key.inbound_id for e in edge_data]
//     inbound_edge_names = {p.id: p.value for p in name_data}

//     self.render(
//         "article.html",
//         article_name=article_name,
//         article_id=article_id,
//         vertex_data=vertex_data[0],
//         edge_count=edge_count,
//         inbound_edge_ids=inbound_edge_ids,
//         inbound_edge_names=inbound_edge_names,
//     )


#[derive(Serialize)]
struct ArticleArguments {
    // article_name: String,
    // article_id: Uuid,
    // edge_count: u64,
    // vertex_data: ...,
    // inbound_edge_ids: ...,
}

#[get("/article?<name>")]
pub fn article(name: &RawStr) -> Template {
    // let article_id = util::article_uuid(name);
    // let vertex_query = indradb::SpecificVertexQuery { ids: vec![article_id] };

    // let mut req = self.client.bulk_insert_request();
    // indradb_proto::util::from_bulk_insert_items(
    //     &self.buf,
    //     req.get().init_items(self.buf.len() as u32)
    // )?;

    // Template::render("article", ArticleArguments {
    //     //
    // })
    Template::render("index", IndexArguments)
}
