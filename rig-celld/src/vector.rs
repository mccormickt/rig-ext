//! Vector validation, encoding, and score normalization shared by backends.

/// Errors in the common vector contract.
#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum VectorError {
    /// The configured vector width is outside the backend range.
    #[error("vector dimensions must be between 1 and {max}, got {actual}")]
    InvalidDimensions { actual: usize, max: usize },
    /// An embedding does not match the configured width.
    #[error("expected an embedding with {expected} dimensions, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    /// A value cannot be represented as a finite f32.
    #[error("embedding contains a non-finite or out-of-range float at position {0}")]
    InvalidValue(usize),
    /// Cosine distance is undefined for a zero vector.
    #[error("zero embeddings are not supported")]
    ZeroVector,
    /// A backend returned a non-finite distance.
    #[error("backend returned a non-finite cosine distance")]
    InvalidDistance,
}

#[cfg(any(feature = "sqlite-vec", test))]
pub(crate) fn validate_dimensions(dimensions: usize, max: usize) -> Result<(), VectorError> {
    if !(1..=max).contains(&dimensions) {
        return Err(VectorError::InvalidDimensions {
            actual: dimensions,
            max,
        });
    }
    Ok(())
}

#[cfg(any(feature = "sqlite-vec", test))]
pub(crate) fn embedding_f32_le(values: &[f64], dimensions: usize) -> Result<Vec<u8>, VectorError> {
    if values.len() != dimensions {
        return Err(VectorError::DimensionMismatch {
            expected: dimensions,
            actual: values.len(),
        });
    }

    let mut non_zero = false;
    let mut bytes = Vec::with_capacity(values.len() * size_of::<f32>());
    for (index, value) in values.iter().enumerate() {
        let value = *value as f32;
        if !value.is_finite() {
            return Err(VectorError::InvalidValue(index));
        }
        non_zero |= value != 0.0;
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    if !non_zero {
        return Err(VectorError::ZeroVector);
    }
    Ok(bytes)
}

#[cfg(any(feature = "sqlite-vec", test))]
pub(crate) fn cosine_score(distance: f64) -> Result<f64, VectorError> {
    if !distance.is_finite() {
        return Err(VectorError::InvalidDistance);
    }
    Ok((1.0 - distance).clamp(-1.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::{VectorError, cosine_score, embedding_f32_le, validate_dimensions};

    #[test]
    fn dimensions_are_bounded() {
        assert_eq!(
            validate_dimensions(0, 8),
            Err(VectorError::InvalidDimensions { actual: 0, max: 8 })
        );
        assert!(validate_dimensions(1, 8).is_ok());
        assert!(validate_dimensions(8, 8).is_ok());
        assert_eq!(
            validate_dimensions(9, 8),
            Err(VectorError::InvalidDimensions { actual: 9, max: 8 })
        );
    }

    #[test]
    fn encoding_is_little_endian_f32() {
        let expected = [1.0_f32.to_le_bytes(), (-0.5_f32).to_le_bytes()].concat();
        assert_eq!(embedding_f32_le(&[1.0, -0.5], 2).ok(), Some(expected));
    }

    #[test]
    fn encoding_rejects_bad_shape_values_and_zero() {
        assert!(matches!(
            embedding_f32_le(&[1.0], 2),
            Err(VectorError::DimensionMismatch { .. })
        ));
        assert!(matches!(
            embedding_f32_le(&[f64::NAN], 1),
            Err(VectorError::InvalidValue(0))
        ));
        assert!(matches!(
            embedding_f32_le(&[f64::MAX], 1),
            Err(VectorError::InvalidValue(0))
        ));
        assert_eq!(
            embedding_f32_le(&[0.0, -0.0], 2),
            Err(VectorError::ZeroVector)
        );
        assert_eq!(
            embedding_f32_le(&[f64::MIN_POSITIVE], 1),
            Err(VectorError::ZeroVector)
        );
    }

    #[test]
    fn cosine_scores_are_finite_and_clamped() {
        assert_eq!(cosine_score(0.0).ok(), Some(1.0));
        assert_eq!(cosine_score(1.0).ok(), Some(0.0));
        assert_eq!(cosine_score(2.0).ok(), Some(-1.0));
        assert_eq!(cosine_score(-0.1).ok(), Some(1.0));
        assert_eq!(cosine_score(2.1).ok(), Some(-1.0));
        assert_eq!(cosine_score(f64::NAN), Err(VectorError::InvalidDistance));
    }
}
