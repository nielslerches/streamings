use std::collections::HashMap;

mod sql;
mod values;
mod planner;
mod relations;
mod executors;

fn main() {
    let catalog = relations::Catalog {
        relations: HashMap::new(),
    };

    let result = sql::parse_statement(b"SELECT ebid FROM pageviews");

    match result {
        Ok(tuple) => {
            let (_, statement) = tuple;
            println!("{statement:?}");

            let sql::Statement::Select(query) = statement;

            println!("{query:?}");
            println!("{:?}", query.select_items);
        }
        Err(err) => {
            println!("{err}")
        }
    }
}
