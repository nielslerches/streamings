use std::borrow::BorrowMut;

use crate::{sql::{Statement, Query}, relations::{Catalog, self}};


pub fn execute_statement(catalog: &mut Catalog, statement: Statement) {
    match statement {
        Statement::Select(query) => {
            execute_query(catalog.clone(), query)
        }
        Statement::CreateKinesisStream(relation_ident, kinesis_stream_name, kinesis_stream_consumer_name) => {
            execute_create_kinesis_stream(catalog, relation_ident, kinesis_stream_name, kinesis_stream_consumer_name);
        }
    }
}

fn execute_query(catalog: Catalog, query: Query) {
    println!("{query:?}")
}

fn plan_query(catalog: Catalog, query: Query) {
}

fn execute_create_kinesis_stream(catalog: &mut Catalog, relation_ident: String, kinesis_stream_name: String, kinesis_stream_consumer_name: String) {
    catalog.relations.insert(relation_ident, relations::RelationDefinition::KinesisStream { kinesis_stream_name, kinesis_stream_consumer_name });
}
