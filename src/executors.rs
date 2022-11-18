use crate::{sql::{Statement, Query}, relations::Catalog};


pub fn execute_statement(catalog: Catalog, statement: Statement) {
    match statement {
        Statement::Select(query) => {
            execute_query(catalog, query)
        }
    }
}

fn execute_query(catalog: Catalog, query: Query) {
    let query_plan = plan_query(catalog, query);
}

fn plan_query(catalog: Catalog, query: Query) {

}
