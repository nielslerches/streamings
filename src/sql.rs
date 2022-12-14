use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{multispace0, multispace1},
        is_alphabetic,
    },
    combinator::{eof, opt},
    multi::separated_list1,
    sequence::{delimited, preceded, terminated},
    IResult,
};

#[derive(Debug, Clone)]
pub enum Statement {
    Select(Query),
    CreateKinesisStream(String, String, String),
}

#[derive(Debug, Clone)]
pub struct Query {
    pub select_items: Vec<SelectItem>,
    pub from_ident: Option<String>,
    pub where_condition: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    NamedExpr(Expr, String),
    Expr(Expr),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Ident(String),
    FunctionCall(String, Vec<Expr>),
    String(String),
}

pub fn parse_statement(input: &[u8]) -> IResult<&[u8], Statement> {
    let (input, statement) = alt((
        |input| {
            let (input, (relation_ident, kinesis_stream_name, kinesis_stream_consumer_name)) =
                parse_create_kinesis_stream(input)?;

            Result::Ok((
                input,
                Statement::CreateKinesisStream(
                    relation_ident,
                    kinesis_stream_name,
                    kinesis_stream_consumer_name,
                ),
            ))
        },
        |input| {
            let (input, query) = parse_query(input)?;

            Result::Ok((input, Statement::Select(query)))
        },
    ))(input)?;

    let (input, _) = eof(input)?;

    return IResult::Ok((input, statement));
}

fn parse_query(input: &[u8]) -> IResult<&[u8], Query> {
    let (input, _) = tag_no_case("SELECT")(input)?;

    let (input, _) = multispace0(input)?;

    let (input, select_items) = separated_list1(
        delimited(multispace0, tag(","), multispace0),
        parse_select_item,
    )(input)?;

    let (input, from_ident) = opt(|input| {
        let (input, _) = multispace0(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace0(input)?;
        parse_ident(input)
    })(input)?;

    let (input, where_condition) = opt(|input| {
        let (input, _) = multispace0(input)?;
        let (input, _) = tag_no_case("WHERE")(input)?;
        let (input, _) = multispace0(input)?;
        parse_expr(input)
    })(input)?;

    IResult::Ok((
        input,
        Query {
            select_items,
            from_ident,
            where_condition,
        },
    ))
}

fn parse_select_item(input: &[u8]) -> IResult<&[u8], SelectItem> {
    alt((
        |input| {
            let (input, expr) = parse_expr(input)?;
            let (input, _) = multispace1(input)?;
            let (input, _) = tag_no_case("AS")(input)?;
            let (input, _) = multispace1(input)?;
            let (input, ident) = parse_ident(input)?;

            IResult::Ok((input, SelectItem::NamedExpr(expr, ident)))
        },
        |input| {
            let (input, expr) = parse_expr(input)?;

            IResult::Ok((input, SelectItem::Expr(expr)))
        },
    ))(input)
}

fn parse_expr(input: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        |input| {
            let (input, (ident, parsed_exprs)) = parse_function_call(input)?;
            IResult::Ok((input, Expr::FunctionCall(ident, parsed_exprs)))
        },
        |input| {
            let (input, string) = parse_string(input)?;
            IResult::Ok((input, Expr::String(string)))
        },
        |input| {
            let (input, ident) = parse_ident(input)?;
            IResult::Ok((input, Expr::Ident(ident)))
        },
    ))(input)
}

fn parse_ident(input: &[u8]) -> IResult<&[u8], String> {
    let (input, ident) = take_while1(|ch| is_alphabetic(ch) || ch == '_' as u8)(input)?;

    return IResult::Ok((input, std::str::from_utf8(ident).unwrap().to_string()));
}

fn parse_function_call(input: &[u8]) -> IResult<&[u8], (String, Vec<Expr>)> {
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

fn parse_create_kinesis_stream(input: &[u8]) -> IResult<&[u8], (String, String, String)> {
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

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, bytes) =
        delimited(tag("'"), take_while1(|chr| chr != '\'' as u8), tag("'"))(input)?;

    IResult::Ok((input, std::str::from_utf8(bytes).unwrap().to_string()))
}
