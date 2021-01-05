use std::error::Error as StdError;

use common;

use indradb_proto as proto;
use indradb::VertexQueryExt;
use tera::{Tera, Context};
use actix_web::{web, App, HttpServer, error, http::StatusCode, HttpResponse, http::header, dev::HttpResponseBuilder};
use serde::{Serialize, Deserialize};
use derive_more::{Display, Error};
use tokio::task;

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

struct AppState {
    templates: Tera
}

impl AppState {
    fn new() -> Result<Self, Box<dyn StdError>> {
        let mut templates = Tera::default();
        templates.add_raw_template("article.tera", ARTICLE_TEMPLATE)?;
        Ok(AppState { templates })
    }
}

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

impl error::ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponseBuilder::new(self.status_code())
            .set_header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::Client { err: _ } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ArticleNotFound { name: _ } => StatusCode::NOT_FOUND
        }
    }
}

async fn index() -> HttpResponse {
    HttpResponse::Ok().content_type("text/html").body(INDEX)
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

async fn article(state: web::Data<AppState>, web::Query(query): web::Query<ArticleQueryParams>) -> Result<HttpResponse, Error> {
    let article_id = common::article_uuid(&query.name);
    let vertex_query = indradb::SpecificVertexQuery::single(article_id);

    let template_context = task::LocalSet::new().run_until(async move {
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

        let context = Context::from_serialize(&ArticleTemplateArgs {
            article_name: query.name,
            article_id: article_id.to_string(),
            edge_count,
            inbound_edges
        }).unwrap();

        Ok(context)
    }).await?;

    let rendered = state.templates.render("article.tera", &template_context).unwrap();
    Ok(HttpResponse::Ok().content_type("text/html").body(rendered))
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let _server = common::Server::start()?;

    let local = task::LocalSet::new();
    let sys = actix_web::rt::System::run_in_tokio("server", &local);

    HttpServer::new(|| {
        App::new()
            .data(AppState::new().unwrap())
            .route("/", web::get().to(index))
            .route("/article", web::get().to(article))
    }).bind("127.0.0.1:8080")?.run().await?;

    sys.await?;

    Ok(())
}