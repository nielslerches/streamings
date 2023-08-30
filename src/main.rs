use std::collections::HashMap;

use nom_locate::LocatedSpan;
use nom_recursive::RecursiveInfo;
use rusoto_core::Region;
use rusoto_kinesis::KinesisClient;

mod definitions;
mod executors;
mod sql;
mod planners;

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

    use std::env::args;

    let mut arguments = args().collect::<Vec<String>>().clone();

    let input = arguments.pop().unwrap();

    let (_, statements) =
        sql::parse_statements(LocatedSpan::new_extra(input.as_str(), RecursiveInfo::new()))
            .expect("could not parse statements");

    for statement in statements {
        executors::execute_statement(catalog, statement).await;
    }
}
