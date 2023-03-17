use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Catalog {
    pub relations: HashMap<String, RelationDefinition>,
    pub functions: HashMap<String, FunctionDefinition>,
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

#[derive(Debug, Clone)]
pub enum FunctionDefinition {
    NativeFunction(NativeFunction),
}

pub type NativeFunction = fn(args: Vec<serde_json::Value>) -> serde_json::Value;
