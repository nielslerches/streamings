use futures_util::stream::StreamExt;
use nom::AsBytes;
use rusoto_kinesis::{
    DescribeStreamConsumerInput, DescribeStreamInput, Kinesis, KinesisClient,
    SubscribeToShardEventStreamItem,
};

use crate::{
    definitions::{self, Catalog, FunctionDefinition},
    sql::{Expr, Query, SelectItem, Statement},
};

type Record = serde_json::Map<String, serde_json::Value>;

pub async fn execute_statement(
    catalog: &mut Catalog,
    kinesis_client: &KinesisClient,
    statement: Statement,
) {
    match statement {
        Statement::Select(query) => execute_query(catalog, kinesis_client, &query).await,
        Statement::CreateKinesisStream(
            relation_ident,
            kinesis_stream_name,
            kinesis_stream_consumer_name,
        ) => {
            execute_create_kinesis_stream(
                catalog,
                kinesis_client,
                relation_ident,
                &kinesis_stream_name,
                kinesis_stream_consumer_name,
            )
            .await
        }
    }
}

async fn execute_query(catalog: &Catalog, kinesis_client: &KinesisClient, query: &Query) {
    if let Some(from_ident) = &query.from_ident {
        if let Some(relation_definition) = catalog.relations.get(from_ident) {
            match relation_definition {
                definitions::RelationDefinition::KinesisStream(kinesis_stream) => {
                    let mut shard_ids = Vec::new();

                    let next_token = None;

                    let mut list_shards_output = kinesis_client
                        .list_shards(rusoto_kinesis::ListShardsInput {
                            exclusive_start_shard_id: None,
                            max_results: None,
                            next_token,
                            shard_filter: None,
                            stream_creation_timestamp: None,
                            stream_name: Some(kinesis_stream.kinesis_stream_name.clone()),
                        })
                        .await
                        .unwrap();

                    shard_ids.extend(
                        list_shards_output
                            .shards
                            .unwrap()
                            .iter()
                            .map(|shard| shard.shard_id.clone()),
                    );

                    while let Some(next_token_inner) = list_shards_output.next_token {
                        list_shards_output = kinesis_client
                            .list_shards(rusoto_kinesis::ListShardsInput {
                                exclusive_start_shard_id: None,
                                max_results: None,
                                next_token: Some(next_token_inner),
                                shard_filter: None,
                                stream_creation_timestamp: None,
                                stream_name: None,
                            })
                            .await
                            .unwrap();

                        shard_ids.extend(
                            list_shards_output
                                .shards
                                .unwrap()
                                .iter()
                                .map(|shard| shard.shard_id.clone()),
                        );
                    }

                    let mut event_streams = Vec::new();

                    for shard_id in shard_ids.as_slice() {
                        let subscribe_to_shard_output = kinesis_client
                            .subscribe_to_shard(rusoto_kinesis::SubscribeToShardInput {
                                consumer_arn: kinesis_stream.kinesis_stream_consumer_arn.clone(),
                                shard_id: shard_id.to_string(),
                                starting_position: rusoto_kinesis::StartingPosition {
                                    sequence_number: None,
                                    timestamp: None,
                                    type_: "TRIM_HORIZON".to_string(),
                                },
                            })
                            .await
                            .unwrap();

                        event_streams.push(subscribe_to_shard_output.event_stream);
                    }

                    use futures_util::stream::select_all::select_all;

                    let mut consolidated_event_stream = select_all(event_streams);

                    while let Some(event_stream_item) = consolidated_event_stream.next().await {
                        if let Ok(subscribe_to_shard_event_stream_item) = event_stream_item {
                            match subscribe_to_shard_event_stream_item {
                                SubscribeToShardEventStreamItem::SubscribeToShardEvent(
                                    subscribe_to_shard_event,
                                ) => {
                                    for record in subscribe_to_shard_event.records.iter() {
                                        if let Ok(input) = serde_json::from_slice::<
                                            serde_json::Map<String, serde_json::Value>,
                                        >(
                                            record.data.as_bytes()
                                        ) {
                                            if let Some(expr) = &query.where_condition {
                                                if !match evaluate_expr(catalog, &input, expr) {
                                                    serde_json::Value::Array(value) => {
                                                        value.len() > 0
                                                    }
                                                    serde_json::Value::Bool(value) => value,
                                                    serde_json::Value::Null => false,
                                                    serde_json::Value::Number(value) => {
                                                        value.as_f64().unwrap() != 0.0
                                                    }
                                                    serde_json::Value::Object(value) => {
                                                        value.len() > 0
                                                    }
                                                    serde_json::Value::String(value) => {
                                                        value.len() > 0
                                                    }
                                                } {
                                                    continue;
                                                }
                                            }

                                            let mut output = serde_json::Map::new();

                                            for (index, select_item) in
                                                query.select_items.iter().enumerate()
                                            {
                                                match select_item {
                                                    SelectItem::NamedExpr(expr, alias) => {
                                                        output.insert(
                                                            alias.to_string(),
                                                            evaluate_expr(catalog, &input, expr),
                                                        );
                                                    }
                                                    SelectItem::Expr(expr) => match expr {
                                                        Expr::Ident(ident) => {
                                                            output.insert(
                                                                ident.to_string(),
                                                                evaluate_expr(
                                                                    catalog, &input, expr,
                                                                ),
                                                            );
                                                        }
                                                        Expr::FunctionCall(function_name, _) => {
                                                            output.insert(
                                                                function_name.to_string(),
                                                                evaluate_expr(
                                                                    catalog, &input, expr,
                                                                ),
                                                            );
                                                        }
                                                        Expr::String(string) => {
                                                            output.insert(
                                                                format!("column{index}")
                                                                    .to_string(),
                                                                serde_json::Value::String(
                                                                    string.to_string(),
                                                                ),
                                                            );
                                                        }
                                                    },
                                                }
                                            }

                                            let output_json =
                                                serde_json::to_string(&output).unwrap();
                                            println!("{output_json}");
                                        } else {
                                            continue;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        } else {
            println!("{from_ident} is not available in the catalog.")
        }
    }
}

fn evaluate_expr(
    catalog: &Catalog,
    record: &Record,
    expr: &Expr,
) -> serde_json::Value {
    match expr {
        Expr::FunctionCall(function_name, function_call_exprs) => {
            match catalog.functions.get(function_name) {
                Some(function_definition) => {
                    let function_call_args = function_call_exprs
                        .iter()
                        .map(|expr| evaluate_expr(catalog, record, expr))
                        .collect::<Vec<serde_json::Value>>();

                    match function_definition {
                        FunctionDefinition::NativeFunction(native_function) => {
                            native_function(function_call_args)
                        }
                    }
                }
                None => {
                    println!("there is no function named {function_name}");
                    serde_json::Value::Null
                }
            }
        }
        Expr::Ident(ident) => {
            let x = record.get(ident);

            match x {
                Some(value) => value.to_owned(),
                None => serde_json::Value::Null,
            }
        }
        Expr::String(string) => serde_json::Value::String(string.to_string()),
    }
}

async fn execute_create_kinesis_stream(
    catalog: &mut Catalog,
    kinesis_client: &KinesisClient,
    relation_ident: String,
    kinesis_stream_name: &String,
    kinesis_stream_consumer_name: String,
) {
    let stream_description = kinesis_client
        .describe_stream(DescribeStreamInput {
            exclusive_start_shard_id: None,
            limit: None,
            stream_name: kinesis_stream_name.to_string(),
        })
        .await
        .unwrap()
        .stream_description;

    let consumer_description = kinesis_client
        .describe_stream_consumer(DescribeStreamConsumerInput {
            consumer_arn: None,
            consumer_name: Some(kinesis_stream_consumer_name),
            stream_arn: Some(stream_description.stream_arn.clone()),
        })
        .await
        .unwrap()
        .consumer_description;

    let kinesis_stream = definitions::KinesisStream {
        kinesis_stream_name: kinesis_stream_name.clone(),
        kinesis_stream_arn: consumer_description.stream_arn,
        kinesis_stream_consumer_arn: consumer_description.consumer_arn,
    };

    catalog.relations.insert(
        relation_ident,
        definitions::RelationDefinition::KinesisStream(kinesis_stream),
    );
}
