use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Catalog {
    pub relations: HashMap<String, RelationDefinition>,
}

#[derive(Debug, Clone)]
pub enum RelationDefinition {
    KinesisStream { kinesis_stream_name: String, kinesis_stream_consumer_name: String },
}
