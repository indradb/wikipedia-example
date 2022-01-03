use std::error::Error as StdError;
use std::time::Instant;

use indradb_proto as proto;

pub async fn run(mut client: proto::Client) -> Result<(), Box<dyn StdError>> {
    let start_time = Instant::now();
    client.execute_plugin("centrality", serde_json::Value::Null).await?;
    println!("finished in {} seconds", start_time.elapsed().as_secs());
    Ok(())
}
