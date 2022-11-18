use std::collections::HashMap;

pub struct Catalog {
    pub relations: HashMap<String, RelationDefinition>,
}

pub enum RelationDefinition {
    KinesisStream { kinesis_stream_name: String, kinesis_stream_consumer_name: String },
}
