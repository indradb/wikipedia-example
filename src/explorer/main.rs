use std::error::Error as StdError;
use std::u32;

use common;

use indradb::VertexQueryExt;
use tera::{Tera, Context};
use capnp::Error as CapnpError;
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
    #[display(fmt = "capnp error: {}", err)]
    Capnp { err: CapnpError },
    #[display(fmt = "article not found: {}", name)]
    ArticleNotFound { name: String }
}

impl From<CapnpError> for Error {
    fn from(err: CapnpError) -> Self {
        Error::Capnp { err }
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
            Error::Capnp { err: _ } => StatusCode::INTERNAL_SERVER_ERROR,
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
        let client = common::client().await?;

        let trans = client.transaction_request().send().pipeline.get_transaction();
        
        let vertex_data_future = {
            let mut req = trans.get_vertices_request();
            indradb_proto::util::from_vertex_query(&vertex_query.clone().into(), req.get().init_q());
            req.send().promise
        };

        let edge_count_future = {
            let mut req = trans.get_edge_count_request();
            req.get().set_id(article_id.as_bytes());
            req.get().set_direction(indradb_proto::util::from_edge_direction(indradb::EdgeDirection::Outbound));
            req.send().promise
        };

        let edges_future = {
            let mut req = trans.get_edges_request();
            indradb_proto::util::from_edge_query(&vertex_query.outbound(u32::MAX).into(), req.get().init_q());
            req.send().promise
        };

        let vertex_data = {
            let res = vertex_data_future.await?;
            let list = res.get()?.get_result()?;
            let list: Result<Vec<indradb::Vertex>, CapnpError> =
                list.into_iter().map(|reader| indradb_proto::util::to_vertex(&reader)).collect();
            list?
        };

        if vertex_data.len() == 0 {
            return Err(Error::ArticleNotFound { name: query.name.clone() });
        }
        
        let edge_count = {
            let res = edge_count_future.await?;
            res.get()?.get_result()
        };

        let edges = {
            let res = edges_future.await?;
            let list = res.get()?.get_result()?;
            let list: Result<Vec<indradb::Edge>, CapnpError> =
                list.into_iter().map(|reader| indradb_proto::util::to_edge(&reader)).collect();
            list?
        };

        let name_data = {
            let mut req = trans.get_vertex_properties_request();
            let q = indradb::VertexPropertyQuery::new(
                indradb::SpecificVertexQuery::new(edges.iter().map(|e| e.key.inbound_id).collect()).into(),
                "name"
            );
            indradb_proto::util::from_vertex_property_query(&q.into(), req.get().init_q());
            let res = req.send().promise.await?;
            let list = res.get()?.get_result()?;
            let list: Result<Vec<indradb::VertexProperty>, CapnpError> = list
                .into_iter()
                .map(|reader| indradb_proto::util::to_vertex_property(&reader))
                .collect();
            list?
        };

        let inbound_edges = name_data.iter()
            .map(|p| (p.id.to_string(), p.value.to_string()))
            .collect();

        let context = Context::from_serialize(&ArticleTemplateArgs {
            article_name: query.name,
            article_id: article_id.to_string(),
            edge_count: edge_count,
            inbound_edges
        }).unwrap();

        Ok(context)
    }).await?;

    let rendered = state.templates.render("article.tera", &template_context).unwrap();
    Ok(HttpResponse::Ok().content_type("text/html").body(rendered))
}

pub async fn run() -> Result<(), Box<dyn StdError>> {
    
    Ok(())
}

#[tokio::main(basic_scheduler)]
pub async fn main() -> Result<(), Box<dyn StdError>> {
    let mut server = common::Server::start()?;

    let local = task::LocalSet::new();
    let sys = actix_web::rt::System::run_in_tokio("server", &local);

    let job_result = HttpServer::new(|| {
        App::new()
            .data(AppState::new().unwrap())
            .route("/", web::get().to(index))
            .route("/article", web::get().to(article))
    }).bind("127.0.0.1:8080")?.run().await;

    let sys_result = sys.await;

    let stop_result = server.stop();
    job_result?;
    sys_result?;
    stop_result?;
    Ok(())
}