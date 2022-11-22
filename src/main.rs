use std::collections::HashMap;

use rusoto_core::Region;
use rusoto_kinesis::KinesisClient;

mod executors;
mod relations;
mod sql;
mod values;

#[tokio::main]
async fn main() {
    let catalog = &mut relations::Catalog {
        relations: HashMap::new(),
    };

    let kinesis_client = KinesisClient::new(Region::EuWest1);

    use std::env::args;

    let mut arguments = args().collect::<Vec<String>>().clone();

    let kinesis_stream_consumer_name = arguments.pop().unwrap();
    let kinesis_stream_name = arguments.pop().unwrap();

    let (_, statement) =
        sql::parse_statement(format!("CREATE KINESIS STREAM longboat '{kinesis_stream_name}' '{kinesis_stream_consumer_name}'").as_bytes()).unwrap();

    println!("{statement:?}");

    executors::execute_statement(catalog, &kinesis_client, statement).await;

    let (_, statement) = sql::parse_statement(b"SELECT ebid FROM longboat").unwrap();

    executors::execute_statement(catalog, &kinesis_client, statement).await;

    println!("{catalog:?}")
}
