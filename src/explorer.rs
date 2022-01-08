use std::convert::Infallible;
use std::error::Error as StdError;

use indradb::VertexQueryExt;
use indradb_proto as proto;
use serde::Deserialize;
use tera::{Context as TeraContext, Tera};
use warp::{http, reject, reply, Filter};

const INDEX: &str = r#"
<form method="get" action="/article">
    <input name="name" value="" type="text" />
    <button type="submit" name="action" value="get_article">Get Article</button>
</form>
"#;

const ARTICLE_TEMPLATE: &str = r#"
<h1>{{ article_name }}</h1>
<ul>
    <li>id: {{ article_id }}</li>
    <li>edge count: {{ edge_count }}</li>
</ul>

<h3>Properties</h3>
<table>
    <tr>
        <th>name</th>
        <th>value</th>
    </tr>
    {% for prop in properties %}
        <tr>
            <td>{{ prop.0 }}</td>
            <td>{{ prop.1 }}</td>
        </tr>
    {% endfor %}
</table>

{% if linked_articles %}
    <h3>Linked articles</h3>
    <table>
        <tr>
            <th>id</th>
            <th>name</th>
        </tr>
        {% for article in linked_articles %}
            <tr>
                <td>{{ article.0 }}</td>
                <td><a href="/article?name={{ article.1 | urlencode }}">{{ article.1 }}</a></td>
            </tr>
        {% endfor %}
    </table>
{% endif %}
"#;

#[derive(Debug)]
enum Error {
    Client { err: proto::ClientError },
    ArticleNotFound { name: String },
}

impl reject::Reject for Error {}

fn map_result<T>(result: Result<T, proto::ClientError>) -> Result<T, warp::Rejection> {
    result.map_err(|err| reject::custom(Error::Client { err }))
}

async fn handle_rejection(err: reject::Rejection) -> Result<impl warp::Reply, Infallible> {
    let (status, message) = if let Some(err) = err.find::<Error>() {
        match err {
            Error::Client { err } => {
                let message = format!("internal error: {}", err);
                (http::StatusCode::INTERNAL_SERVER_ERROR, message)
            }
            Error::ArticleNotFound { name } => {
                let message = format!("article not found: {}", name);
                (http::StatusCode::NOT_FOUND, message)
            }
        }
    } else {
        (
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "UNHANDLED_REJECTION".to_string(),
        )
    };

    Ok(reply::with_status(reply::html(message), status))
}

#[derive(Deserialize)]
struct ArticleQueryParams {
    name: String,
}

async fn handle_index() -> Result<impl warp::Reply, Infallible> {
    Ok(reply::html(INDEX))
}

async fn handle_article(
    mut client: proto::Client,
    tera: Tera,
    query: ArticleQueryParams,
) -> Result<impl warp::Reply, warp::Rejection> {
    let name_identifier = indradb::Identifier::new("name").unwrap();

    let article_id = {
        let property_value = serde_json::Value::String(query.name.clone());
        let q = indradb::PropertyValueVertexQuery::new(name_identifier.clone(), property_value);
        let vertices = map_result(client.get_vertices(q.into()).await)?;
        if vertices.is_empty() {
            return Err(reject::custom(Error::ArticleNotFound {
                name: query.name.clone(),
            }));
        }
        assert_eq!(vertices.len(), 1);
        vertices[0].id
    };
    let article_query = indradb::SpecificVertexQuery::single(article_id);

    let edge_count = map_result(
        client
            .get_edge_count(article_id, None, indradb::EdgeDirection::Outbound)
            .await,
    )?;

    let properties = {
        let properties = map_result(client.get_all_vertex_properties(article_query.clone().into()).await)?;
        assert_eq!(properties.len(), 1);
        properties[0]
            .props
            .iter()
            .map(|p| (p.name.to_string(), p.value.to_string()))
            .collect::<Vec<(String, String)>>()
    };

    let linked_articles = {
        let edges = map_result(client.get_edges(article_query.outbound().into()).await)?;

        let q = indradb::VertexPropertyQuery::new(
            indradb::SpecificVertexQuery::new(edges.iter().map(|e| e.key.inbound_id).collect()).into(),
            name_identifier,
        );
        map_result(client.get_vertex_properties(q).await)?
            .iter()
            .map(|p| {
                if let serde_json::Value::String(s) = &p.value {
                    (p.id.to_string(), s.clone())
                } else {
                    unreachable!();
                }
            })
            .collect::<Vec<(String, String)>>()
    };

    let mut context = TeraContext::new();
    context.insert("article_name", &query.name);
    context.insert("article_id", &article_id.to_string());
    context.insert("edge_count", &edge_count);
    context.insert("properties", &properties);
    context.insert("linked_articles", &linked_articles);
    let rendered = tera.render("article.html", &context).unwrap();
    Ok(reply::html(rendered))
}

fn with_client(client: proto::Client) -> impl Filter<Extract = (proto::Client,), Error = Infallible> + Clone {
    warp::any().map(move || client.clone())
}

fn with_templating(tera: Tera) -> impl Filter<Extract = (Tera,), Error = Infallible> + Clone {
    warp::any().map(move || tera.clone())
}

pub async fn run(client: proto::Client, port: u16) -> Result<(), Box<dyn StdError>> {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![("article.html", ARTICLE_TEMPLATE)])?;

    let index_route = warp::path::end().and(warp::get()).and_then(handle_index);

    let article_route = warp::path("article")
        .and(warp::get())
        .and(with_client(client.clone()))
        .and(with_templating(tera.clone()))
        .and(warp::query::<ArticleQueryParams>())
        .and_then(handle_article);

    let routes = index_route.or(article_route).recover(handle_rejection);

    warp::serve(routes).run(([127, 0, 0, 1], port)).await;

    Ok(())
}
