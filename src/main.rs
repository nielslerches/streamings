use std::collections::HashMap;

use rusoto_core::Region;
use rusoto_kinesis::KinesisClient;

mod definitions;
mod executors;
mod sql;

#[tokio::main]
async fn main() {
    let catalog = &mut definitions::Catalog {
        relations: HashMap::new(),
        functions: HashMap::new(),
    };

    catalog.functions.insert(
        "lower".to_string(),
        definitions::FunctionDefinition::NativeFunction(|args| {
            let value = args
                .first()
                .expect("lower() needs to be called with 1 argument");

            match value {
                serde_json::Value::String(s) => serde_json::Value::String(s.to_lowercase()),
                _ => {
                    println!("lower() argument needs to be a String");
                    serde_json::Value::String("".to_string())
                }
            }
        }),
    );

    let kinesis_client = KinesisClient::new(Region::EuWest1);

    use std::env::args;

    let mut arguments = args().collect::<Vec<String>>().clone();

    let input = arguments.pop().unwrap();

    let (_, statements) = sql::parse_statements(input.as_str()).unwrap();

    for statement in statements {
        executors::execute_statement(catalog, &kinesis_client, statement).await;
    }
}
