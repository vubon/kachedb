//! `kachedb-vector` — SQ8 (Scalar Quantization 8-bit) compression and ADC kernels.
//!
//! Compresses 32-bit floating point vectors into 8-bit unsigned integers (4x memory reduction)
//! with min-max scalar quantization and asymmetric distance computation (ADC).

/// Quantization configuration and compression mode for vector spaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum QuantizationMode {
    /// Full 32-bit floating point precision (no compression).
    #[default]
    None,
    /// 8-bit scalar quantization with asymmetric query scoring.
    SQ8,
}

impl QuantizationMode {
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("SQ8") {
            Some(Self::SQ8)
        } else if s.eq_ignore_ascii_case("NONE") || s.eq_ignore_ascii_case("FP32") {
            Some(Self::None)
        } else {
            None
        }
    }
}

/// SQ8 Scalar Quantizer for 4x vector memory reduction.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sq8Quantizer;

impl Sq8Quantizer {
    /// Encodes a 32-bit float vector into 8-bit unsigned integers along with (min, max) bounds.
    pub fn encode(fp32: &[f32]) -> (Vec<u8>, f32, f32) {
        if fp32.is_empty() {
            return (Vec::new(), 0.0, 0.0);
        }

        let mut min_val = f32::INFINITY;
        let mut max_val = f32::NEG_INFINITY;

        for &x in fp32 {
            if x < min_val {
                min_val = x;
            }
            if x > max_val {
                max_val = x;
            }
        }

        let diff = max_val - min_val;
        let mut encoded = Vec::with_capacity(fp32.len());

        if diff <= 1e-12 {
            encoded.resize(fp32.len(), 128);
            return (encoded, min_val, max_val);
        }

        let scale = 255.0 / diff;
        for &x in fp32 {
            let u = ((x - min_val) * scale).round().clamp(0.0, 255.0) as u8;
            encoded.push(u);
        }

        (encoded, min_val, max_val)
    }

    /// Decodes an 8-bit quantized slice back to an approximate 32-bit float vector.
    pub fn decode(bytes: &[u8], min: f32, max: f32) -> Vec<f32> {
        let diff = max - min;
        if diff <= 1e-12 {
            return vec![min; bytes.len()];
        }

        let scale = diff / 255.0;
        let mut decoded = Vec::with_capacity(bytes.len());
        for &b in bytes {
            let x = min + (b as f32) * scale;
            decoded.push(x);
        }
        decoded
    }

    /// Asymmetric Dot Product (ADC kernel): evaluates dot product of FP32 query with SQ8 stored vector
    /// in $O(D)$ without allocating an intermediate decoded float vector.
    ///
    /// $$q \cdot x = \sum_i q_i (\min + u_i \cdot \text{scale}) = \min \sum_i q_i + \text{scale} \sum_i (q_i \cdot u_i)$$
    pub fn asymmetric_dot_product(query: &[f32], stored_u8: &[u8], min: f32, max: f32) -> f32 {
        let len = query.len().min(stored_u8.len());
        if len == 0 {
            return 0.0;
        }

        let diff = max - min;
        if diff <= 1e-12 {
            let sum_q: f32 = query[..len].iter().sum();
            return min * sum_q;
        }

        let scale = diff / 255.0;
        let mut sum_q = 0.0f32;
        let mut sum_qu = 0.0f32;

        for i in 0..len {
            let q = query[i];
            let u = stored_u8[i] as f32;
            sum_q += q;
            sum_qu += q * u;
        }

        min * sum_q + scale * sum_qu
    }

    /// Asymmetric Squared Euclidean Distance ($L_2^2$): evaluates $\sum (q_i - x_i)^2$.
    pub fn asymmetric_l2_squared(query: &[f32], stored_u8: &[u8], min: f32, max: f32) -> f32 {
        let len = query.len().min(stored_u8.len());
        if len == 0 {
            return 0.0;
        }

        let diff = max - min;
        if diff <= 1e-12 {
            let mut sum = 0.0f32;
            for &q in &query[..len] {
                let d = q - min;
                sum += d * d;
            }
            return sum;
        }

        let scale = diff / 255.0;
        let mut sum = 0.0f32;
        for i in 0..len {
            let x = min + (stored_u8[i] as f32) * scale;
            let d = query[i] - x;
            sum += d * d;
        }
        sum
    }

    /// Asymmetric Cosine Similarity between a unit-normalized FP32 query and SQ8 stored vector.
    pub fn asymmetric_cosine_similarity(
        query_normalized: &[f32],
        stored_u8: &[u8],
        min: f32,
        max: f32,
    ) -> f32 {
        let dot = Self::asymmetric_dot_product(query_normalized, stored_u8, min, max);
        // Estimate norm of quantized vector
        let len = query_normalized.len().min(stored_u8.len());
        let diff = max - min;
        let norm_stored = if diff <= 1e-12 {
            (min * min * len as f32).sqrt()
        } else {
            let scale = diff / 255.0;
            let mut sq_sum = 0.0f32;
            for &b in &stored_u8[..len] {
                let x = min + (b as f32) * scale;
                sq_sum += x * x;
            }
            sq_sum.sqrt()
        };

        if norm_stored > 1e-12 {
            (dot / norm_stored).clamp(-1.0, 1.0)
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sq8_encode_decode_precision() {
        let original = vec![0.12, -0.45, 0.88, 0.0, 1.23, -1.05];
        let (encoded, min, max) = Sq8Quantizer::encode(&original);
        assert_eq!(encoded.len(), original.len());

        let decoded = Sq8Quantizer::decode(&encoded, min, max);
        assert_eq!(decoded.len(), original.len());

        let max_err = (max - min) / 255.0;
        for (a, b) in original.iter().zip(decoded.iter()) {
            assert!(
                (a - b).abs() <= max_err + 1e-4,
                "diff |{a} - {b}| > {max_err}"
            );
        }
    }

    #[test]
    fn test_asymmetric_dot_product_matches_decoded() {
        let vec_a = vec![0.2, -0.5, 0.9, 0.1, -0.3, 0.7];
        let vec_b = vec![0.8, 0.3, -0.1, 0.4, 0.6, -0.2];

        let (encoded_b, min, max) = Sq8Quantizer::encode(&vec_b);
        let decoded_b = Sq8Quantizer::decode(&encoded_b, min, max);

        // Exact dot product of vec_a with decoded_b
        let expected_dot: f32 = vec_a.iter().zip(decoded_b.iter()).map(|(x, y)| x * y).sum();
        let adc_dot = Sq8Quantizer::asymmetric_dot_product(&vec_a, &encoded_b, min, max);

        assert!((expected_dot - adc_dot).abs() < 1e-4);
    }

    #[test]
    fn test_constant_vector_quantization() {
        let val = std::f32::consts::PI;
        let original = vec![val; 16];
        let (encoded, min, max) = Sq8Quantizer::encode(&original);
        assert_eq!(encoded.len(), 16);
        assert_eq!(min, val);
        assert_eq!(max, val);

        let decoded = Sq8Quantizer::decode(&encoded, min, max);
        for &x in &decoded {
            assert!((x - val).abs() < 1e-5);
        }
    }
}
