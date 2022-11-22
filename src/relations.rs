use std::{collections::HashMap};

#[derive(Debug, Clone)]
pub struct Catalog {
    pub relations: HashMap<String, RelationDefinition>,
}

#[derive(Debug, Clone)]
pub enum RelationDefinition {
    KinesisStream(KinesisStream),
}

#[derive(Debug, Clone)]
pub struct KinesisStream {
    pub kinesis_stream_name: String,
    pub kinesis_stream_arn: String,
    pub kinesis_stream_consumer_arn: String,
}
