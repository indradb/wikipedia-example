use std::error::Error as StdError;
use std::convert::Infallible;

use common;

use indradb_proto as proto;
use indradb::VertexQueryExt;
use serde::{Serialize, Deserialize};
use derive_more::{Display, Error};
use handlebars::Handlebars;
use warp::Filter;

const INDEX: &str = r#"
<form method="get" action="/article">
    <input name="name" value="" type="text" />
    <button type="submit" name="action" value="get_article">Get Article</button>
</form>
"#;

const ARTICLE_TEMPLATE: &str = r#"
<h1>{{ article_name }}</h1>
<h3>Properties</h3>
<ul>
    <li>id: {{ article_id }}</li>
    <li>edge count: {{ edge_count }}</li>
</ul>
{% if inbound_edges %}
    <h3>Edges</h3>
    <table>
        <tr>
            <th>id</th>
            <th>name</th>
        </tr>
        {% for edge in inbound_edges %}
            <tr>
                <td>{{ edge.0 }}</td>
                <td><a href="/?article={{ edge.1 | urlencode }}&action=get_article">{{ edge.1 }}</a></td>
            </tr>
        {% endfor %}
    </table>
{% endif %}
"#;

#[derive(Debug, Display, Error)]
enum Error {
    #[display(fmt = "client error: {}", err)]
    Client { err: proto::ClientError },
    #[display(fmt = "article not found: {}", name)]
    ArticleNotFound { name: String }
}

impl From<proto::ClientError> for Error {
    fn from(err: proto::ClientError) -> Self {
        Error::Client { err }
    }
}

impl warp::reject::Reject for Error {}

async fn handle_rejection(err: warp::reject::Rejection) -> Result<impl warp::Reply, Infallible> {
    let (status, message) = if let Some(err) = err.find::<Error>() {
        match err {
            Error::Client { err } => {
                let message = format!("internal error: {}", err);
                (warp::http::StatusCode::INTERNAL_SERVER_ERROR, message)
            },
            Error::ArticleNotFound { name } => {
                let message = format!("article not found: {}", name);
                (warp::http::StatusCode::NOT_FOUND, message)
            }
        }
    } else {
        (warp::http::StatusCode::INTERNAL_SERVER_ERROR, "UNHANDLED_REJECTION".to_string())
    };

    Ok(warp::reply::with_status(warp::reply::html(message), status))
}

#[derive(Deserialize)]
struct ArticleQueryParams {
    name: String
}

#[derive(Serialize)]
struct ArticleTemplateArgs {
    article_name: String,
    article_id: String,
    edge_count: u64,
    inbound_edges: Vec<(String, String)>
}

async fn handle_index() -> Result<impl warp::Reply, Error> {
    Ok(warp::reply::html(INDEX))
}

async fn handle_article(query: ArticleQueryParams) -> Result<impl warp::Reply, Error> {
    let article_id = common::article_uuid(&query.name);
    let vertex_query = indradb::SpecificVertexQuery::single(article_id);

    let mut client = common::client().await?;
    let mut trans = client.transaction().await?;

    let vertices = trans.get_vertices(vertex_query.clone()).await?;
    if vertices.len() == 0 {
        return Err(Error::ArticleNotFound { name: query.name.clone() });
    }

    let edge_count = trans.get_edge_count(article_id, None, indradb::EdgeDirection::Outbound).await?;
    let edges = trans.get_edges(vertex_query.outbound()).await?;

    let name = {
        let q = indradb::VertexPropertyQuery::new(
            indradb::SpecificVertexQuery::new(edges.iter().map(|e| e.key.inbound_id).collect()).into(),
            "name"
        );
        trans.get_vertex_properties(q).await?
    };

    let inbound_edges = name.iter()
        .map(|p| (p.id.to_string(), p.value.to_string()))
        .collect();

    let template_args = ArticleTemplateArgs {
        article_name: query.name,
        article_id: article_id.to_string(),
        edge_count,
        inbound_edges
    };

    let mut hb = Handlebars::new();
    hb.register_template_string("article.html", ARTICLE_TEMPLATE).unwrap();
    let render = hb
        .render("article.html", &template_args)
        .unwrap_or_else(|err| err.to_string());
    Ok(warp::reply::html(render))
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let _server = common::Server::start()?;

    let index_route = warp::path::end()
        .and(warp::get())
        .and_then(handle_index);

    let article_route = warp::path("article")
        .and(warp::get())
        .and(warp::query::<ArticleQueryParams>())
        .and_then(handle_article);

    let routes = index_route
        .or(article_route)
        .recover(handle_rejection);

    warp::serve(routes).run(([127, 0, 0, 1], 8080)).await;

    Ok(())
}