use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{multispace0, multispace1},
        is_alphabetic,
    },
    combinator::{eof, opt},
    multi::{many1, separated_list1},
    sequence::{delimited, preceded, terminated},
    IResult,
};
use nom_locate::LocatedSpan;
use nom_recursive::{recursive_parser, RecursiveInfo};

#[derive(Debug, Clone)]
pub enum Statement {
    Select(Query),
    CreateKinesisStream(String, String, String),
    Explain(Query),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Query {
    pub select_items: Vec<SelectItem>,
    pub from_items: Vec<FromItem>,
    pub where_condition: Option<Expr>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum SelectItem {
    NamedExpr(Expr, String),
    Expr(Expr),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Expr {
    Ident(String),
    FunctionCall(String, Vec<Expr>),
    String(String),
    Number(serde_json::Number),
    BinaryOperation(Box<Expr>, BinaryOperator, Box<Expr>),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Gte,
    Lte,
    Gt,
    Lt,
    Eq,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum FromItem {
    SubQuery(Query),
    Ident(String),
    Values(Vec<Vec<Expr>>),
}

type Span<'a> = LocatedSpan<&'a str, RecursiveInfo>;

pub fn parse_statements(input: Span) -> IResult<Span, Vec<Statement>> {
    let (input, statements) = many1(parse_statement)(input)?;

    let (input, _) = eof(input)?;

    Ok((input, statements))
}

pub fn parse_statement(input: Span) -> IResult<Span, Statement> {
    let (input, statement) = terminated(
        alt((
            |input| {
                let (input, (relation_ident, kinesis_stream_name, kinesis_stream_consumer_name)) =
                    parse_create_kinesis_stream(input)?;

                Ok((
                    input,
                    Statement::CreateKinesisStream(
                        relation_ident,
                        kinesis_stream_name,
                        kinesis_stream_consumer_name,
                    ),
                ))
            },
            |input| {
                let (input, _) = tag_no_case("EXPLAIN")(input)?;
                let (input, _) = multispace0(input)?;
                let (input, query) = parse_query(input)?;

                Ok((input, Statement::Explain(query)))
            },
            |input| {
                let (input, query) = parse_query(input)?;

                Ok((input, Statement::Select(query)))
            },
        )),
        parse_statement_terminator,
    )(input)?;

    return Ok((input, statement));
}

#[recursive_parser]
fn parse_query(s: Span) -> IResult<Span, Query> {
    let (input, _) = tag_no_case("SELECT")(s)?;

    let (input, _) = multispace0(input)?;

    let (input, select_items) = separated_list1(
        delimited(multispace0, tag(","), multispace0),
        parse_select_item,
    )(input)?;

    let (input, _) = multispace0(input)?;

    let (input, from_items) = parse_from_items(input)?;

    let (input, where_condition) = opt(|input| {
        let (input, _) = multispace0(input)?;
        let (input, _) = tag_no_case("WHERE")(input)?;
        let (input, _) = multispace0(input)?;
        parse_expr(input)
    })(input)?;

    Ok((
        input,
        Query {
            select_items,
            from_items,
            where_condition,
        },
    ))
}

fn parse_from_items(input: Span) -> IResult<Span, Vec<FromItem>> {
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    separated_list1(
        tag(","),
        alt((
            delimited(
                tag("("),
                |input| {
                    let (input, _) = tag_no_case("VALUES")(input)?;
                    let (input, _) = multispace1(input)?;
                    let (input, values) = separated_list1(tag(","), |input| {
                        let (input, columns) =
                            delimited(tag("("), separated_list1(tag(","), parse_expr), tag(")"))(
                                input,
                            )?;

                        Ok((input, columns))
                    })(input)?;

                    Ok((input, FromItem::Values(values)))
                },
                tag(")"),
            ),
            |input| {
                let (input, ident) = parse_ident(input)?;
                Ok((input, FromItem::Ident(ident)))
            },
            |input| {
                let (input, query) = delimited(tag("("), parse_query, tag(")"))(input)?;
                Ok((input, FromItem::SubQuery(query)))
            },
        )),
    )(input)
}

fn parse_select_item(input: Span) -> IResult<Span, SelectItem> {
    alt((
        |input| {
            let (input, expr) = parse_expr(input)?;
            let (input, _) = multispace1(input)?;
            let (input, _) = tag_no_case("AS")(input)?;
            let (input, _) = multispace1(input)?;
            let (input, ident) = parse_ident(input)?;

            Ok((input, SelectItem::NamedExpr(expr, ident)))
        },
        |input| {
            let (input, expr) = parse_expr(input)?;

            Ok((input, SelectItem::Expr(expr)))
        },
    ))(input)
}

fn parse_expr(input: Span) -> IResult<Span, Expr> {
    alt((
        |input| {
            let (input, (left_expr, binary_operator, right_expr)) = parse_binary_operation(input)?;
            Ok((
                input,
                Expr::BinaryOperation(Box::new(left_expr), binary_operator, Box::new(right_expr)),
            ))
        },
        |input| {
            let (input, (ident, parsed_exprs)) = parse_function_call(input)?;
            Ok((input, Expr::FunctionCall(ident, parsed_exprs)))
        },
        |input| {
            let (input, string) = parse_string(input)?;
            Ok((input, Expr::String(string)))
        },
        |input| {
            let (input, number) = parse_number(input)?;
            Ok((input, Expr::Number(number)))
        },
        |input| {
            let (input, ident) = parse_ident(input)?;
            Ok((input, Expr::Ident(ident)))
        },
    ))(input)
}

fn parse_ident(input: Span) -> IResult<Span, String> {
    let (input, ident) = take_while1(|ch: char| is_alphabetic(ch as u8) || ch == '_')(input)?;

    return Ok((input, ident.to_string()));
}

fn parse_function_call(input: Span) -> IResult<Span, (String, Vec<Expr>)> {
    let (input, ident) = parse_ident(input)?;

    let (input, parsed_exprs) = preceded(
        tag("("),
        terminated(
            opt(separated_list1(
                delimited(opt(multispace0), tag(","), opt(multispace0)),
                parse_expr,
            )),
            tag(")"),
        ),
    )(input)?;

    IResult::Ok((
        input,
        (
            ident,
            if let Some(parsed_exprs) = parsed_exprs {
                parsed_exprs
            } else {
                vec![]
            },
        ),
    ))
}

fn parse_create_kinesis_stream(input: Span) -> IResult<Span, (String, String, String)> {
    let (input, _) = tag_no_case("CREATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("KINESIS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("STREAM")(input)?;
    let (input, _) = multispace1(input)?;

    let (input, relation_ident) = terminated(parse_ident, multispace1)(input)?;

    let (input, kinesis_stream_name) = terminated(parse_string, multispace1)(input)?;

    let (input, kinesis_stream_consumer_name) = parse_string(input)?;

    IResult::Ok((
        input,
        (
            relation_ident,
            kinesis_stream_name,
            kinesis_stream_consumer_name,
        ),
    ))
}

fn parse_string(input: Span) -> IResult<Span, String> {
    let (input, string) = delimited(tag("'"), take_while1(|chr| chr != '\''), tag("'"))(input)?;

    IResult::Ok((input, string.to_string()))
}

fn parse_number(input: Span) -> IResult<Span, serde_json::Number> {
    let (input, digits) = take_while1(char::is_numeric)(input)?;

    IResult::Ok((input, digits.parse().unwrap()))
}

#[recursive_parser]
fn parse_binary_operation(s: Span) -> IResult<Span, (Expr, BinaryOperator, Expr)> {
    let (s, left_expr) = parse_expr(s)?;
    let (s, _) = multispace0(s)?;
    let (s, binary_operator) = parse_binary_operator(s)?;
    let (s, _) = multispace0(s)?;
    let (s, right_expr) = parse_expr(s)?;

    Ok((s, (left_expr, binary_operator, right_expr)))
}

fn parse_binary_operator(input: Span) -> IResult<Span, BinaryOperator> {
    alt((
        |input| {
            let (input, _) = tag("+")(input)?;
            IResult::Ok((input, BinaryOperator::Add))
        },
        |input| {
            let (input, _) = tag("-")(input)?;
            IResult::Ok((input, BinaryOperator::Sub))
        },
        |input| {
            let (input, _) = tag("*")(input)?;
            IResult::Ok((input, BinaryOperator::Mul))
        },
        |input| {
            let (input, _) = tag("/")(input)?;
            IResult::Ok((input, BinaryOperator::Div))
        },
        |input| {
            let (input, _) = tag("=")(input)?;
            IResult::Ok((input, BinaryOperator::Eq))
        },
        |input| {
            let (input, _) = tag(">=")(input)?;
            IResult::Ok((input, BinaryOperator::Gte))
        },
        |input| {
            let (input, _) = tag("<=")(input)?;
            IResult::Ok((input, BinaryOperator::Lte))
        },
        |input| {
            let (input, _) = tag(">")(input)?;
            IResult::Ok((input, BinaryOperator::Gt))
        },
        |input| {
            let (input, _) = tag("<")(input)?;
            IResult::Ok((input, BinaryOperator::Lt))
        },
    ))(input)
}

fn parse_statement_terminator(input: Span) -> IResult<Span, ()> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag(";")(input)?;
    let (input, _) = multispace0(input)?;

    IResult::Ok((input, ()))
}
