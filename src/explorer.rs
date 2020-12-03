use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter, Error as FmtError};
use std::u32;

use crate::util;

use indradb::{VertexQueryExt, EdgeQueryExt};
use indradb_proto::util as proto_util;
use serde::Serialize;
use warp::Filter;
use tera::Tera;
use capnp::Error as CapnpError;

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
    <li>type: {{ vertex_data.t }}</li>
    <li>edge count: {{ edge_count }}</li>
</ul>

{% if inbound_edge_ids %}
    <h3>Edges</h3>

    <table>
        <tr>
            <th>id</th>
            <th>name</th>
        </tr>
        {% for edge_id in inbound_edge_ids %}
            <tr>
                <td>{{ edge_id }}</td>
                <td><a href="/?article={{ inbound_edge_names[edge_id] | urlencode }}&action=get_article">{{ inbound_edge_names[edge_id] }}</a></td>
            </tr>
        {% endfor %}
    </table>
{% endif %}
"#;

lazy_static! {
    static ref TEMPLATES: Tera = {
        let mut tera = Tera::default();
        tera.add_raw_template("article.tera", ARTICLE_TEMPLATE);
        tera
    };
}

#[derive(Debug)]
struct ServerError {
    message: String
}
impl warp::reject::Reject for ServerError {}

fn map_err<T, E: StdError>(result: Result<T, E>) -> Result<T, warp::Rejection> {
    result.map_err(|err| {
        warp::reject::custom(ServerError { message: format!("{}", err) })
    })
}

async fn article(query: HashMap<String, String>) -> Result<impl warp::Reply, warp::Rejection> {
    let name_default = "".to_string();
    let name = query.get("name").unwrap_or(&name_default);
    let article_id = util::article_uuid(&name);
    let vertex_query = indradb::SpecificVertexQuery::single(article_id);

    // TODO: make this middleware
    let client = map_err(util::retrying_client().await)?;

    let trans = client.transaction_request().send().pipeline.get_transaction();
    
    let vertex_data_future = {
        let mut req = trans.get_vertices_request();
        proto_util::from_vertex_query(&vertex_query.clone().into(), req.get().init_q());
        req.send().promise
    };

    let edge_count_future = {
        let mut req = trans.get_edge_count_request();
        req.get().set_id(article_id.as_bytes());
        req.get().set_direction(proto_util::from_edge_direction(indradb::EdgeDirection::Outbound));
        req.send().promise
    };

    let edges_future = {
        let mut req = trans.get_edges_request();
        proto_util::from_edge_query(&vertex_query.outbound(u32::MAX).into(), req.get().init_q());
        req.send().promise
    };

    let vertex_data = {
        let res = map_err(vertex_data_future.await)?;
        let list = map_err(map_err(res.get())?.get_result())?;
        let list: Result<Vec<indradb::Vertex>, CapnpError> =
            list.into_iter().map(|reader| proto_util::to_vertex(&reader)).collect();
        map_err(list)?
    };

    if vertex_data.len() == 0 {
        return Err(warp::reject::not_found());
    }
    
    let edge_count = {
        let res = map_err(edge_count_future.await)?;
        map_err(res.get())?.get_result()
    };

    let edges = {
        let res = map_err(edges_future.await)?;
        let list = map_err(map_err(res.get())?.get_result())?;
        let list: Result<Vec<indradb::Edge>, CapnpError> =
            list.into_iter().map(|reader| proto_util::to_edge(&reader)).collect();
        map_err(list)?
    };

    let name_data = {
        let mut req = trans.get_vertex_properties_request();
        let q = indradb::VertexPropertyQuery::new(
            indradb::SpecificVertexQuery::new(edges.iter().map(|e| e.key.inbound_id).collect()).into(),
            "name"
        );
        proto_util::from_vertex_property_query(&q.into(), req.get().init_q());
        let res = map_err(req.send().promise.await)?;
        let list = map_err(map_err(res.get())?.get_result())?;
        let list: Result<Vec<indradb::VertexProperty>, CapnpError> = list
            .into_iter()
            .map(|reader| proto_util::to_vertex_property(&reader))
            .collect();
        map_err(list)?
    };

    todo!();
}

pub async fn run() {
    let index = warp::get()
        .and(warp::path(""))
        .map(|| INDEX);

    let article = warp::get()
        .and(warp::path("article"))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(article);

    warp::serve(index.or(article)).run(([127, 0, 0, 1], 8000)).await;
}
