use std::{default, sync::Arc};

use nom::AsBytes;
use rusoto_core::Region;
use rusoto_kinesis::{
    DescribeStreamConsumerInput, DescribeStreamInput, Kinesis, KinesisClient,
    SubscribeToShardEventStreamItem,
};

use crate::{
    relations::{self, Catalog},
    sql::{Expr, Query, SelectItem, Statement},
};

struct KinesisStreamSelector {
    kinesis_stream: relations::KinesisStream,
}

pub async fn execute_statement(
    catalog: &mut Catalog,
    kinesis_client: &KinesisClient,
    statement: Statement,
) {
    match statement {
        Statement::Select(query) => execute_query(catalog, kinesis_client, query).await,
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

async fn execute_query(catalog: &Catalog, kinesis_client: &KinesisClient, query: Query) {
    if let Some(from_ident) = query.from_ident {
        if let Some(relation_definition) = catalog.relations.get(&from_ident) {
            if let relations::RelationDefinition::KinesisStream(kinesis_stream) =
                relation_definition
            {
                let mut shard_ids = Vec::new();

                let next_token = None;

                let mut list_shards_output = kinesis_client
                    .list_shards(rusoto_kinesis::ListShardsInput {
                        exclusive_start_shard_id: None,
                        max_results: None,
                        next_token: next_token,
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

                match kinesis_client
                    .subscribe_to_shard(rusoto_kinesis::SubscribeToShardInput {
                        consumer_arn: kinesis_stream.kinesis_stream_consumer_arn.clone(),
                        shard_id: shard_ids.first().unwrap().clone(),
                        starting_position: rusoto_kinesis::StartingPosition {
                            sequence_number: None,
                            timestamp: None,
                            type_: "TRIM_HORIZON".to_string(),
                        },
                    })
                    .await
                {
                    Ok(subscribe_to_shard_output) => {
                        let mut event_stream = subscribe_to_shard_output.event_stream;

                        use futures_util::stream::StreamExt;
                        while let Some(event_stream_item) = event_stream.next().await {
                            if let SubscribeToShardEventStreamItem::SubscribeToShardEvent(
                                subscribe_to_shard_event,
                            ) = event_stream_item.unwrap()
                            {
                                for record in subscribe_to_shard_event.records.iter() {
                                    use serde_json;

                                    if let Ok(dto) = serde_json::from_slice::<
                                        serde_json::Map<String, serde_json::Value>,
                                    >(
                                        record.data.as_bytes()
                                    ) {
                                        for select_item in query.select_items.iter() {
                                            match select_item {
                                                SelectItem::Expr(expr) => match expr {
                                                    Expr::Ident(ident) => {
                                                        let value = dto.get(ident);

                                                        println!("{value:?}")
                                                    }
                                                    _ => {
                                                        unimplemented!()
                                                    }
                                                },
                                            }
                                        }
                                    } else {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        println!("{err}")
                    }
                }
            } else {
                unimplemented!("only kinesis streams are queryable")
            }
        } else {
            println!("{from_ident} is not available in the catalog.")
        }
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

    let kinesis_stream = relations::KinesisStream {
        kinesis_stream_name: kinesis_stream_name.clone(),
        kinesis_stream_arn: consumer_description.stream_arn,
        kinesis_stream_consumer_arn: consumer_description.consumer_arn,
    };

    catalog.relations.insert(
        relation_ident,
        relations::RelationDefinition::KinesisStream(kinesis_stream),
    );
}
