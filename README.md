# arrow-cast-guess-precision

Cast integer to timestamp with precision guessing options.

Just replace arrow::cast with arrow_cast_guest_precision::cast and everything done.

```rust
let data = vec![1701325744956, 1701325744956];
let array = arrow_array::Int64Array::from(data);
let array = crate::cast(
    &array,
    &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
)
.unwrap();
let nanos = array
    .as_any()
    .downcast_ref::<TimestampNanosecondArray>()
    .unwrap();
assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
```
