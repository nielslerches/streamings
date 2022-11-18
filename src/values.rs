pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Array(Vec<Value>),
}
