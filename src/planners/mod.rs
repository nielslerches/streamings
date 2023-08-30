use std::collections::HashMap;

use crate::{
    definitions::{Catalog, RelationDefinition},
    sql::{Expr, FromItem, Query, SelectItem},
};

#[derive(Debug, Clone)]
pub enum QueryPlan {
    Projection {
        items: HashMap<String, Expr>,
        query: Box<QueryPlan>,
    },
    Selection {
        condition: Expr,
        query: Box<QueryPlan>,
    },
    KinesisStreamScan {
        kinesis_stream_name: String,
        kinesis_stream_consumer_arn: String,
    },
    ValuesScan(Vec<Vec<Expr>>),
    FullJoin(Box<QueryPlan>, Box<QueryPlan>),
    Empty,
}

pub fn plan_query(catalog: &Catalog, query: &Query) -> Result<QueryPlan, String> {
    let mut plan = if query.from_items.len() > 0 {
        let from_items = query.from_items.clone();

        from_items[1..].iter().fold(
            plan_from_item(catalog, from_items.first().unwrap()).expect("could not plan from item"),
            |acc, from_item| {
                QueryPlan::FullJoin(
                    Box::new(acc),
                    Box::new(plan_from_item(catalog, from_item).expect("could not plan from item")),
                )
            },
        )
    } else {
        QueryPlan::Empty
    };

    if let Some(condition) = &query.where_condition {
        plan = QueryPlan::Selection { condition: condition.clone(), query: Box::new(plan) };
    }

    if query.select_items.len() > 0 {
        let mut items = HashMap::new();

        for (i, select_item) in query.select_items.iter().enumerate() {
            let (key, value) = match select_item {
                SelectItem::Expr(expr) => {
                    let key = match expr {
                        Expr::Ident(ident) => ident.clone(),
                        Expr::FunctionCall(name, _) => name.clone(),
                        _ => format!("column{i}"),
                    };
                    (key, expr)
                }
                SelectItem::NamedExpr(expr, name) => (name.clone(), expr),
            };

            if items.insert(key.clone(), value.clone()) != None {
                return Err(format!("{key} already defined in select items"));
            }
        }

        plan = QueryPlan::Projection {
            items,
            query: Box::new(plan),
        };
    }

    Ok(plan)
}

fn plan_from_item(catalog: &Catalog, from_item: &FromItem) -> Result<QueryPlan, String> {
    match from_item {
        FromItem::Ident(ident) => {
            if let Some(relation_definition) = catalog.relations.get(ident) {
                match relation_definition {
                    RelationDefinition::KinesisStream(kinesis_stream) => {
                        Ok(QueryPlan::KinesisStreamScan {
                            kinesis_stream_name: kinesis_stream.kinesis_stream_name.clone(),
                            kinesis_stream_consumer_arn: kinesis_stream.kinesis_stream_consumer_arn.clone(),
                        })
                    }
                }
            } else {
                Err(format!("unrecognized relation {ident}"))
            }
        }
        FromItem::SubQuery(query) => plan_query(catalog, query),
        FromItem::Values(values) => Ok(QueryPlan::ValuesScan(values.clone())),
    }
}
