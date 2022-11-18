use std::{collections::HashMap};

mod sql;
mod values;
mod planner;
mod relations;
mod executors;

fn main() {
    let catalog = &mut relations::Catalog {
        relations: HashMap::new(),
    };

    let (_, statement) = sql::parse_statement(b"CREATE KINESIS STREAM longboat 'longboat' 'consumer'").unwrap();

    println!("{statement:?}");

    executors::execute_statement(catalog, statement);

    println!("{catalog:?}")
}
