use std::convert::Infallible;
use std::error::Error as StdError;

use indradb::QueryExt;
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
<h1>{{ article_name }} ({{ article_id }})</h1>

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
    let property_value = indradb::Json::new(serde_json::Value::String(query.name.clone()));
    let base_q = indradb::VertexWithPropertyValueQuery::new(name_identifier, property_value);

    let results = map_result(client.get(base_q.clone().include().properties().unwrap()).await)?;
    assert_eq!(results.len(), 2);

    let article_id = if let indradb::QueryOutputValue::Vertices(article_vertices) = &results[0] {
        if article_vertices.is_empty() {
            return Err(reject::custom(Error::ArticleNotFound {
                name: query.name.clone(),
            }));
        }
        assert_eq!(article_vertices.len(), 1);
        article_vertices[0].id
    } else {
        unreachable!();
    };

    let article_properties = if let indradb::QueryOutputValue::VertexProperties(article_properties) = &results[1] {
        assert_eq!(article_properties.len(), 1);
        article_properties[0]
            .props
            .iter()
            .map(|p| (p.name.to_string(), p.value.to_string()))
            .collect::<Vec<(String, String)>>()
    } else {
        unreachable!();
    };

    let linked_articles = {
        let q = base_q
            .clone()
            .outbound()
            .unwrap()
            .inbound()
            .unwrap()
            .properties()
            .unwrap()
            .name(name_identifier);
        let results = map_result(client.get(q).await)?;
        let linked_article_properties = indradb::util::extract_vertex_properties(results).unwrap();

        let mut linked_articles = Vec::with_capacity(linked_article_properties.len());
        for vertex_props in linked_article_properties.into_iter() {
            assert_eq!(vertex_props.props.len(), 1);
            if let serde_json::Value::String(s) = &*vertex_props.props[0].value.0 {
                linked_articles.push((vertex_props.vertex.id.to_string(), s.clone()));
            } else {
                unreachable!();
            }
        }

        linked_articles
    };

    let mut context = TeraContext::new();
    context.insert("article_name", &query.name);
    context.insert("article_id", &article_id.to_string());
    context.insert("properties", &article_properties);
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
