mod sql;

fn main() {
    let result = sql::parse_statement(b"SELECT foobar , barfoo(foobar, foobar(barfoo))");

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
