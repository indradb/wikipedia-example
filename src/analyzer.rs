use std::error::Error as StdError;
use std::time::Instant;

use indradb_proto as proto;
use serde_json::json;

pub async fn run(mut client: proto::Client) -> Result<(), Box<dyn StdError>> {
    let start_time = Instant::now();
    client
        .execute_plugin(
            "centrality",
            json!({
                "max_iterations": 50,
                "cache_edges": true,
                "max_delta": 0.015
            }),
        )
        .await?;
    println!("finished in {} seconds", start_time.elapsed().as_secs());
    Ok(())
}
