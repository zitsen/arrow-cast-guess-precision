use arrow_array::{make_array, new_empty_array, new_null_array, Array, ArrayRef, Int64Array};
use arrow_schema::{ArrowError, DataType, TimeUnit};

const ONE_HANDRED_YEARS_S: i64 = 86400 * 365 * 200;
const ONE_HANDRED_YEARS_MS: i64 = 1000 * 86400 * 365 * 200;
const ONE_HANDRED_YEARS_US: i64 = 1000 * 1000 * 86400 * 365 * 200;
fn detect_precision(timestamp: i64) -> TimeUnit {
    let timestamp = timestamp.abs();
    if timestamp > ONE_HANDRED_YEARS_US {
        return TimeUnit::Nanosecond;
    }
    if timestamp > ONE_HANDRED_YEARS_MS {
        return TimeUnit::Microsecond;
    }
    if timestamp > ONE_HANDRED_YEARS_S {
        return TimeUnit::Millisecond;
    }
    TimeUnit::Second
}

// The array should be an array of i64.
fn detect_precision_in_array(array: &dyn Array) -> Option<TimeUnit> {
    let v = array.as_any().downcast_ref::<Int64Array>().unwrap();
    v.into_iter().flatten().next().map(detect_precision)
}

pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to_type, &CastOptions::default())
}

#[derive(Debug, Clone)]
pub struct TimestampCastOptions {
    /// If true, try to guess the precision of the timestamp from integers.
    ///
    /// Caster will first convert the integer to i64 and then guess the precision.
    pub guess_timestamp_precision: bool,
    /// If true, caster use the timezone in target type. If false, caster will use UTC.
    pub use_timezone_as_is: bool,
}

impl Default for TimestampCastOptions {
    fn default() -> Self {
        Self {
            guess_timestamp_precision: true,
            use_timezone_as_is: true,
        }
    }
}

pub struct CastOptions<'a> {
    pub safe: bool,
    pub timestamp_options: TimestampCastOptions,
    pub format_options: arrow_cast::display::FormatOptions<'a>,
}

impl Default for CastOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl CastOptions<'_> {
    pub fn new() -> Self {
        Self {
            safe: true,
            timestamp_options: TimestampCastOptions::default(),
            format_options: arrow_cast::display::FormatOptions::default(),
        }
    }
}

impl<'r, 'a> From<&'r CastOptions<'a>> for arrow_cast::CastOptions<'r> {
    fn from(options: &'r CastOptions) -> arrow_cast::CastOptions<'r> {
        arrow_cast::CastOptions {
            safe: options.safe.clone(),
            format_options: options.format_options.clone(),
        }
    }
}

pub fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    let from_type = array.data_type();
    if from_type == to_type {
        return Ok(make_array(array.to_data()));
    }
    if array.len() == 0 {
        return Ok(new_empty_array(to_type));
    }
    if from_type == &Null {
        return Ok(new_null_array(to_type, array.len()));
    }

    // to_type, Timestamp(unit, tz)) {
    match (from_type, to_type) {
        (
            // Convert to second precision integer.
            Int8 | Int16 | Int32 | UInt8 | UInt32 | Float16 | Float32 | UInt16,
            Timestamp(unit, tz),
        ) => {
            let tz = if cast_options.timestamp_options.use_timezone_as_is {
                tz.clone()
            } else {
                None
            };
            let array = arrow_cast::cast(array, &Int64)?;
            if cast_options.timestamp_options.guess_timestamp_precision {
                let array = arrow_cast::cast(&array, &Timestamp(TimeUnit::Second, tz))?;
                return arrow_cast::cast_with_options(&array, to_type, &cast_options.into());
            } else {
                let array = arrow_cast::cast(&array, &Timestamp(unit.clone(), tz))?;
                return arrow_cast::cast_with_options(&array, to_type, &cast_options.into());
            }
        }

        (Binary | FixedSizeBinary(_) | LargeBinary | Utf8 | LargeUtf8, _) => {
            let string_to_ts = arrow_cast::cast_with_options(array, to_type, &cast_options.into())?;
            if string_to_ts.null_count() == string_to_ts.len() {
                if let Ok(array) =
                    arrow_cast::cast_with_options(array, &Int64, &cast_options.into())
                {
                    if array.null_count() < array.len() {
                        // Indicate that the string is timestamp integer.
                        return cast_with_options(array.as_ref(), to_type, cast_options);
                    }
                }
            }
            return Ok(string_to_ts);
        }
        (Int64 | UInt64 | Float64, Timestamp(unit, tz)) => {
            let array = arrow_cast::cast(array, &Int64)?;

            let tz = if cast_options.timestamp_options.use_timezone_as_is {
                tz.clone()
            } else {
                None
            };
            if cast_options.timestamp_options.guess_timestamp_precision {
                let array = arrow_cast::cast(
                    &array,
                    &Timestamp(
                        detect_precision_in_array(&array).unwrap_or_else(|| unit.clone()),
                        tz,
                    ),
                )?;
                return arrow_cast::cast_with_options(&array, to_type, &cast_options.into());
            } else {
                let array = cast(&array, &Timestamp(unit.clone(), tz))?;
                return arrow_cast::cast_with_options(&array, to_type, &cast_options.into());
            }
        }
        _ => arrow_cast::cast_with_options(array, to_type, &cast_options.into()),
    }
}

#[cfg(test)]
mod test {
    use arrow_array::TimestampNanosecondArray;

    use super::*;

    #[test]
    fn test_int_to_timestamp() {
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
        dbg!(nanos);
        assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
        dbg!(array);
    }

    #[test]
    fn test_string_to_timestamp() {
        let string = vec!["1701325744956", "1701325744956"];
        let array = arrow_array::StringArray::from(string);
        let array = crate::cast(
            &array,
            &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        )
        .unwrap();
        let nanos = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        dbg!(nanos);
        assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
        dbg!(array);
    }
    #[test]
    fn test() {
        let now = chrono::Utc::now();
        let ten_years_ago = now - chrono::Duration::days(365 * 100);
        let ten_years_later = now + chrono::Duration::days(365 * 100);

        let mut ints = Vec::new();

        for now in [now, ten_years_ago, ten_years_later] {
            println!("Timestamp {} in ms: {}", now, now.timestamp_millis());
            println!("Timestamp {} in us: {}", now, now.timestamp_micros());
            println!(
                "Timestamp {} in ns: {}",
                now,
                now.timestamp_nanos_opt().unwrap()
            );

            ints.push(now.timestamp_millis());
            ints.push(now.timestamp_micros());
            ints.push(now.timestamp_nanos_opt().unwrap());
            // chrono::DateTime::from_timestamp(now.timestamp_nanos());
        }

        ints.push(i32::MAX as _);

        for i in ints {
            println!("Timestamp {} in {:?}", i, detect_precision(i),);
        }
    }
}
