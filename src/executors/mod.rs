use std::{collections::HashMap, pin::Pin};

use async_recursion::async_recursion;
use futures_util::Future;
use tokio::sync::mpsc;

use crate::{
    definitions::{Catalog, FunctionDefinition, Record},
    planners::{self, plan_query},
    sql::{BinaryOperator, Expr, SelectItem, Statement},
};

pub mod kinesis;

pub async fn execute_statement<'a>(
    catalog: &'a mut Catalog,
    statement: Statement,
) {
    match statement {
        Statement::Select(query) => {
            let (sender, mut receiver) = mpsc::channel(256);

            let plan = plan_query(catalog, &query).unwrap();
            let mut futures = execute_plan(catalog, plan, sender).await;

            tokio_scoped::scope(|scope| {
                for future in futures.drain(0..) {
                    scope.spawn(future);
                }

                scope.spawn(async move {
                    while let Some(record) = receiver.recv().await {
                        let json = serde_json::to_string(&record).unwrap();
                        println!("{json}");
                    }
                });
            });
        }
        Statement::CreateKinesisStream(
            relation_ident,
            kinesis_stream_name,
            kinesis_stream_consumer_name,
        ) => {
            kinesis::execute_create_kinesis_stream(
                catalog,
                relation_ident,
                kinesis_stream_name,
                kinesis_stream_consumer_name,
            )
            .await
        }
        Statement::Explain(query) => {
            println!("{:?}", plan_query(catalog, &query).unwrap());
        }
    }
}

#[async_recursion]
async fn execute_plan<'a>(
    catalog: &'a Catalog,
    plan: planners::QueryPlan,
    sender: mpsc::Sender<Record>,
) -> Vec<Pin<Box<dyn Future<Output = ()> + Send + 'a>>> {
    let mut futures = Vec::new();

    match plan {
        planners::QueryPlan::Empty => (),
        planners::QueryPlan::FullJoin(left, right) => {
            let (left_sender, left_receiver) = mpsc::channel(256);
            let mut left_futures = execute_plan(catalog, *left, left_sender).await;
            futures.append(&mut left_futures);

            let (right_sender, right_receiver) = mpsc::channel(256);
            let mut right_futures =
                execute_plan(catalog, *right, right_sender).await;
            futures.append(&mut right_futures);

            let future = Box::pin(execute_full_join(
                left_receiver,
                right_receiver,
                sender,
            ));
            futures.push(future);
        }
        planners::QueryPlan::KinesisStreamScan {
            kinesis_stream_name,
            kinesis_stream_consumer_arn,
        } => {
            let future = Box::pin(kinesis::execute_read_kinesis_stream(
                kinesis_stream_name,
                kinesis_stream_consumer_arn,
                sender,
            ));
            futures.push(future);
        }
        planners::QueryPlan::Projection { items, query } => {
            let (inner_sender, inner_receiver) = mpsc::channel(256);

            let mut inner_futures =
                execute_plan(catalog, *query, inner_sender).await;

            futures.append(&mut inner_futures);
            futures.push(Box::pin(execute_projection(
                catalog,
                items,
                inner_receiver,
                sender,
            )));
        }
        planners::QueryPlan::Selection { condition, query } => {
            let (inner_sender, inner_receiver) = mpsc::channel(256);

            let mut inner_futures =
                execute_plan(catalog, *query, inner_sender).await;
            futures.append(&mut inner_futures);
            futures.push(Box::pin(execute_filter(
                catalog,
                condition,
                inner_receiver,
                sender,
            )));
        }
        planners::QueryPlan::ValuesScan(values) => {
            let context = Record::new();

            for record in values.iter().map(|row| {
                Record::from_iter(row.iter().enumerate().map(|(index, expr)| {
                    (
                        format!("column{index}"),
                        evaluate_expr(catalog, &context, expr),
                    )
                }))
            }) {
                sender.send(record).await.unwrap();
            }
        }
    }

    futures
}

async fn execute_projection<'a>(
    catalog: &'a Catalog,
    items: HashMap<String, Expr>,
    mut receiver: mpsc::Receiver<Record>,
    sender: mpsc::Sender<Record>,
) {
    while let Some(input_record) = receiver.recv().await {
        let mut output_record = Record::new();

        for (key, expr) in items.iter() {
            let value = evaluate_expr(catalog, &input_record, expr);
            output_record.insert(key.clone(), value);
        }

        sender.send(output_record).await.unwrap();
    }
}

async fn execute_full_join<'a>(
    mut left_receiver: mpsc::Receiver<Record>,
    mut right_receiver: mpsc::Receiver<Record>,
    sender: mpsc::Sender<Record>,
) {
    let mut left_buffer: Vec<Record> = Vec::new();
    let mut right_buffer: Vec<Record> = Vec::new();

    loop {
        if let Some(left_record) = &left_receiver.recv().await {
            left_buffer.push(left_record.clone());

            for right_record in right_buffer.iter() {
                let mut record = left_record.clone();
                record.extend(right_record.clone());
                sender.send(record).await.unwrap();
            }
        } else {
            break;
        }

        if let Some(right_record) = &right_receiver.recv().await {
            right_buffer.push(right_record.clone());

            for left_record in left_buffer.iter() {
                let mut record = left_record.clone();
                record.extend(right_record.clone());
                sender.send(record).await.unwrap();
            }
        } else {
            break;
        }

    }
}

async fn execute_filter<'a>(
    catalog: &'a Catalog,
    expr: Expr,
    mut receiver: mpsc::Receiver<Record>,
    sender: mpsc::Sender<Record>,
) {
    while let Some(record) = receiver.recv().await {
        if let serde_json::Value::Bool(b) = evaluate_expr(catalog, &record, &expr) {
            if b {
                sender.send(record).await.unwrap();
            }
        }
    }
}

fn evaluate_select_items<'a>(
    catalog: &'a Catalog,
    input: &Record,
    select_items: &Vec<SelectItem>,
) -> Record {
    let mut output = serde_json::Map::new();

    for (index, select_item) in select_items.iter().enumerate() {
        match select_item {
            SelectItem::NamedExpr(expr, alias) => {
                output.insert(alias.to_string(), evaluate_expr(catalog, &input, expr));
            }
            SelectItem::Expr(expr) => match expr {
                Expr::Ident(ident) => {
                    output.insert(ident.to_string(), evaluate_expr(catalog, &input, expr));
                }
                Expr::FunctionCall(function_name, _) => {
                    output.insert(
                        function_name.to_string(),
                        evaluate_expr(catalog, &input, expr),
                    );
                }
                _ => {
                    output.insert(
                        format!("column{index}").to_string(),
                        evaluate_expr(catalog, input, expr),
                    );
                }
            },
        }
    }

    output
}

fn evaluate_expr<'a>(catalog: &'a Catalog, record: &Record, expr: &Expr) -> serde_json::Value {
    match expr {
        Expr::FunctionCall(function_name, function_call_exprs) => {
            match catalog.functions.get(function_name) {
                Some(function_definition) => {
                    let function_call_args = function_call_exprs
                        .iter()
                        .map(|expr| evaluate_expr(catalog, record, expr))
                        .collect();

                    match function_definition {
                        FunctionDefinition::NativeFunction(native_function) => {
                            native_function(function_call_args)
                        }
                    }
                }
                None => {
                    eprintln!("there is no function named {function_name}");
                    serde_json::Value::Null
                }
            }
        }
        Expr::Ident(ident) => {
            let x = record.get(ident);

            match x {
                Some(value) => value.to_owned(),
                None => {
                    eprintln!("there is no variable named {ident}");
                    serde_json::Value::Null
                }
            }
        }
        Expr::String(string) => serde_json::Value::String(string.to_string()),
        Expr::Number(number) => serde_json::Value::Number(number.clone()),
        Expr::BinaryOperation(left_expr, binary_operator, right_expr) => {
            let left = evaluate_expr(catalog, record, left_expr);
            let right = evaluate_expr(catalog, record, right_expr);

            match (left, binary_operator, right) {
                (left, BinaryOperator::Eq, right) => serde_json::Value::from(left == right),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Sub,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() - right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Add,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() + right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Div,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() / right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Mul,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() * right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Gt,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() > right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Gte,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() >= right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Lt,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() < right.as_f64().unwrap()),
                (
                    serde_json::Value::Number(left),
                    BinaryOperator::Lte,
                    serde_json::Value::Number(right),
                ) => serde_json::Value::from(left.as_f64().unwrap() <= right.as_f64().unwrap()),
                _ => serde_json::Value::Null,
            }
        }
    }
}
