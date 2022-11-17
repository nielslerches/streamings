use std::{io::Empty, str};

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_till, take_while, take_while1},
    character::{complete::multispace0, is_alphabetic, is_space},
    combinator::{eof, fail, opt},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, terminated},
    Err, IResult,
};

#[derive(Debug)]
enum Statement {
    Select(Query),
}

#[derive(Debug)]
struct Query {
    select_items: Vec<SelectItem>,
}

#[derive(Debug)]
enum SelectItem {
    Expr(Expr),
}

#[derive(Debug)]
enum Expr {
    Ident(String),
    FunctionCall(String, Vec<Expr>),
}

fn parse_statement(input: &[u8]) -> IResult<&[u8], Statement> {
    let (input, statement) = alt((parse_select_statement,))(input)?;

    let (input, _) = eof(input)?;

    return IResult::Ok((input, statement));
}

fn parse_select_statement(input: &[u8]) -> IResult<&[u8], Statement> {
    let (input, query) = parse_query(input)?;

    return IResult::Ok((input, Statement::Select(query)));
}

fn parse_query(input: &[u8]) -> IResult<&[u8], Query> {
    let (input, _) = tag_no_case("SELECT")(input)?;

    let (input, _) = take_while(is_space)(input)?;

    let (input, select_items) = separated_list1(
        delimited(multispace0, tag(","), multispace0),
        parse_select_item,
    )(input)?;

    IResult::Ok((input, Query { select_items }))
}

fn parse_select_item(input: &[u8]) -> IResult<&[u8], SelectItem> {
    let (input, expr) = parse_expr(input)?;

    IResult::Ok((input, SelectItem::Expr(expr)))
}

fn parse_expr(input: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        |input| {
            let (input, (ident, parsed_exprs)) = parse_function_call(input)?;
            Result::Ok((input, Expr::FunctionCall(ident, parsed_exprs)))
        },
        |input| {
            let (input, ident) = parse_ident(input)?;
            Result::Ok((input, Expr::Ident(ident)))
        },
    ))(input)
}

fn parse_ident(input: &[u8]) -> IResult<&[u8], String> {
    let (input, ident) = take_while1(is_alphabetic)(input)?;

    return IResult::Ok((input, str::from_utf8(ident).unwrap().to_string()));
}

fn parse_function_call(input: &[u8]) -> IResult<&[u8], (String, Vec<Expr>)> {
    let (input, ident) = parse_ident(input)?;

    let (input, parsed_exprs) = preceded(
        tag("("),
        terminated(opt(separated_list1(tag(","), parse_expr)), tag(")")),
    )(input)?;

    Result::Ok((
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

fn main() {
    let result = parse_statement(b"SELECT foobar , barfoo(foobar)");

    match result {
        Ok(tuple) => {
            let (_, statement) = tuple;
            println!("{statement:?}");

            let Statement::Select(query) = statement;

            println!("{query:?}");
            println!("{:?}", query.select_items);
        }
        Err(err) => {
            println!("{err}")
        }
    }
}
