use futures_util::stream::StreamExt;
use nom::AsBytes;
use rusoto_core::Region;
use rusoto_kinesis::{
    DescribeStreamConsumerInput, DescribeStreamInput, Kinesis, KinesisClient,
    SubscribeToShardEventStreamItem,
};
use tokio::sync::mpsc;

use crate::definitions::{self, Catalog, Record};

pub async fn execute_read_kinesis_stream(
    kinesis_stream_name: String,
    kinesis_stream_consumer_arn: String,
    sender: mpsc::Sender<Record>,
) {
    let kinesis_client = KinesisClient::new(Region::EuWest1);

    let mut shard_ids = Vec::new();

    let next_token = None;

    let mut list_shards_output = kinesis_client
        .list_shards(rusoto_kinesis::ListShardsInput {
            exclusive_start_shard_id: None,
            max_results: None,
            next_token,
            shard_filter: None,
            stream_creation_timestamp: None,
            stream_name: Some(kinesis_stream_name),
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
                consumer_arn: kinesis_stream_consumer_arn.clone(),
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
                        if let Ok(input) = serde_json::from_slice::<Record>(record.data.as_bytes())
                        {
                            sender.send(input).await.unwrap();
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

pub async fn execute_create_kinesis_stream(
    catalog: &mut Catalog,
    relation_ident: String,
    kinesis_stream_name: String,
    kinesis_stream_consumer_name: String,
) {
    let kinesis_client = KinesisClient::new(Region::EuWest1);

    let stream_description = kinesis_client
        .describe_stream(DescribeStreamInput {
            exclusive_start_shard_id: None,
            limit: None,
            stream_name: kinesis_stream_name.clone(),
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
