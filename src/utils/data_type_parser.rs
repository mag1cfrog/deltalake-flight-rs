use deltalake::kernel::{DataType, PrimitiveType};
use regex::Regex;
use tonic::Status;

/// Parses a type string into Delta Lake's PrimitiveType
/// 
/// Supports various type aliases and formats:
/// - "string" => PrimitiveType::String
/// - "long" | "bigint" => PrimitiveType::Long
/// - "int" | "integer" => PrimitiveType::Integer
/// - "short" | "smallint" => PrimitiveType::Short
/// - "byte" | "tinyint" => PrimitiveType::Byte
/// - "float" | "real" => PrimitiveType::Float
/// - "double" | "doubleprecision" => PrimitiveType::Double
/// - "boolean" | "bool" => PrimitiveType::Boolean
/// - "binary" | "bytes" => PrimitiveType::Binary
/// - "date" => PrimitiveType::Date
/// - "timestamp" => PrimitiveType::Timestamp
/// - "timestamp_ntz" => PrimitiveType::TimestampNtz
/// - "decimal(p,s)" => PrimitiveType::Decimal(p,s)
pub fn parse_data_type(type_str: &str) -> Result<DataType, Status> {
    let lower_type = type_str.trim().to_lowercase();
    let primitive = match lower_type.as_str() {
        "string" => PrimitiveType::String,
        "long" | "bigint" => PrimitiveType::Long,
        "int" | "integer" => PrimitiveType::Integer,
        "short" | "smallint" => PrimitiveType::Short,
        "byte" | "tinyint" => PrimitiveType::Byte,
        "float" | "real" => PrimitiveType::Float,
        "double" | "doubleprecision" => PrimitiveType::Double,
        "boolean" | "bool" => PrimitiveType::Boolean,
        "binary" | "bytes" => PrimitiveType::Binary,
        "date" => PrimitiveType::Date,
        "timestamp" => PrimitiveType::Timestamp,
        "timestamp_ntz" => PrimitiveType::TimestampNtz,
        decimal if decimal.starts_with("decimal") => parse_decimal(decimal)?,
        _ => return Err(Status::invalid_argument(format!(
            "Unsupported data type: {}", type_str
        ))),
    };

    Ok(DataType::Primitive(primitive))
}

fn parse_decimal(decimal_str: &str) -> Result<PrimitiveType, Status> {
    // Make regex more flexible with optional whitespace
    let re = Regex::new(r"decimal\((\d+)\s*,\s*(\d+)\)")
        .map_err(|e| Status::internal(format!("Regex error: {}", e)))?;

    let caps = re.captures(decimal_str)
        .ok_or_else(|| Status::invalid_argument(
            "Decimal type must be in format 'decimal(precision,scale)'"
        ))?;
    
    let precision = caps[1].parse::<u8>()
        .map_err(|_| Status::invalid_argument("Invalid decimal precision"))?;

    let scale = caps[2].parse::<u8>()
    .map_err(|_| Status::invalid_argument("Invalid decimal scale"))?;

    if precision < scale {
        return Err(Status::invalid_argument(
            "Decimal precision cannot be less than scale"
        ));
    }
    
    Ok(PrimitiveType::Decimal(precision, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_types() {
        assert_eq!(
            parse_data_type("STRING").unwrap(),
            DataType::Primitive(PrimitiveType::String)
        );
        
        assert_eq!(
            parse_data_type("BIGINT").unwrap(),
            DataType::Primitive(PrimitiveType::Long)
        );

        assert_eq!(
            parse_data_type("timestamp_ntz").unwrap(),
            DataType::Primitive(PrimitiveType::TimestampNtz)
        );
    }

    #[test]
    fn test_parse_decimal() {
        assert_eq!(
            parse_data_type("decimal(10,2)").unwrap(),
            DataType::Primitive(PrimitiveType::Decimal(10, 2))
        );

        assert!(parse_data_type("decimal(5)").is_err());
        assert!(parse_data_type("decimal(a,b)").is_err());
        assert!(parse_data_type("decimal(5,10)").is_err());
    }

    #[test]
    fn test_invalid_types() {
        assert!(parse_data_type("uuid").is_err());
        assert!(parse_data_type("array<string>").is_err());
    }

    #[test]
    fn test_decimal_variations() {
        // Test different spacing in decimal type
        assert_eq!(
            parse_data_type("decimal(10, 2)").unwrap(),
            DataType::Primitive(PrimitiveType::Decimal(10, 2))
        );
        assert_eq!(
            parse_data_type("DECIMAL(10,2)").unwrap(),
            DataType::Primitive(PrimitiveType::Decimal(10, 2))
        );
        
        // Test edge cases for precision and scale
        assert_eq!(
            parse_data_type("decimal(38,38)").unwrap(),
            DataType::Primitive(PrimitiveType::Decimal(38, 38))
        );
        
        // Test invalid formats
        assert!(parse_data_type("decimal()").is_err());
        assert!(parse_data_type("decimal(,)").is_err());
        assert!(parse_data_type("decimal(10,)").is_err());
        assert!(parse_data_type("decimal(,2)").is_err());
        assert!(parse_data_type("decimal(256,2)").is_err()); // u8 overflow
    }

    #[test]
    fn test_case_insensitivity() {
        assert_eq!(
            parse_data_type("STRING").unwrap(),
            parse_data_type("string").unwrap()
        );
        assert_eq!(
            parse_data_type("INTEGER").unwrap(),
            parse_data_type("integer").unwrap()
        );
        assert_eq!(
            parse_data_type("Boolean").unwrap(),
            parse_data_type("boolean").unwrap()
        );
    }

    #[test]
    fn test_type_aliases() {
        // Test all aliases for numeric types
        assert_eq!(
            parse_data_type("int").unwrap(),
            parse_data_type("integer").unwrap()
        );
        assert_eq!(
            parse_data_type("long").unwrap(),
            parse_data_type("bigint").unwrap()
        );
        assert_eq!(
            parse_data_type("short").unwrap(),
            parse_data_type("smallint").unwrap()
        );
        assert_eq!(
            parse_data_type("byte").unwrap(),
            parse_data_type("tinyint").unwrap()
        );
        assert_eq!(
            parse_data_type("float").unwrap(),
            parse_data_type("real").unwrap()
        );
        assert_eq!(
            parse_data_type("double").unwrap(),
            parse_data_type("doubleprecision").unwrap()
        );
    }

    #[test]
    fn test_whitespace_handling() {
        // Test whitespace before and after type name
        assert_eq!(
            parse_data_type("  string  ").unwrap(),
            DataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            parse_data_type("\tinteger\n").unwrap(),
            DataType::Primitive(PrimitiveType::Integer)
        );
    }
}