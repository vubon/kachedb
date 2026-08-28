//! `kachedb-vector` — Hardware-accelerated SIMD vector math kernels.
//!
//! Provides ARM NEON (`vfmaq_f32`), x86 AVX2/FMA (`_mm256_fmadd_ps`), and scalar fallback
//! implementations for vector dot products, $L_2$ normalization, cosine similarity, and Euclidean distance.

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::*;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

/// Computes the dot product of two float slices using ARM NEON SIMD intrinsics (128-bit registers).
///
/// Unrolled 4-way across 4 accumulator registers processing 16 `f32` values per iteration.
///
/// # Safety
///
/// The caller must ensure that the target CPU supports ARM NEON intrinsics.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
pub unsafe fn dot_product_neon(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    let mut a_ptr = a.as_ptr();
    let mut b_ptr = b.as_ptr();

    unsafe {
        let mut sum0 = vdupq_n_f32(0.0);
        let mut sum1 = vdupq_n_f32(0.0);
        let mut sum2 = vdupq_n_f32(0.0);
        let mut sum3 = vdupq_n_f32(0.0);

        let chunks16 = len / 16;
        for _ in 0..chunks16 {
            let va0 = vld1q_f32(a_ptr);
            let vb0 = vld1q_f32(b_ptr);
            sum0 = vfmaq_f32(sum0, va0, vb0);

            let va1 = vld1q_f32(a_ptr.add(4));
            let vb1 = vld1q_f32(b_ptr.add(4));
            sum1 = vfmaq_f32(sum1, va1, vb1);

            let va2 = vld1q_f32(a_ptr.add(8));
            let vb2 = vld1q_f32(b_ptr.add(8));
            sum2 = vfmaq_f32(sum2, va2, vb2);

            let va3 = vld1q_f32(a_ptr.add(12));
            let vb3 = vld1q_f32(b_ptr.add(12));
            sum3 = vfmaq_f32(sum3, va3, vb3);

            a_ptr = a_ptr.add(16);
            b_ptr = b_ptr.add(16);
        }

        // Combine 4 accumulators into 1
        let sum01 = vaddq_f32(sum0, sum1);
        let sum23 = vaddq_f32(sum2, sum3);
        let mut total_sum = vaddq_f32(sum01, sum23);

        // Process remaining 4-float chunks
        let rem = len % 16;
        let chunks4 = rem / 4;
        for _ in 0..chunks4 {
            let va = vld1q_f32(a_ptr);
            let vb = vld1q_f32(b_ptr);
            total_sum = vfmaq_f32(total_sum, va, vb);
            a_ptr = a_ptr.add(4);
            b_ptr = b_ptr.add(4);
        }

        // Horizontal add of the 4 lanes in total_sum
        let mut acc = vaddvq_f32(total_sum);

        // Process leftover scalars (0..3 floats)
        let tail = rem % 4;
        for i in 0..tail {
            acc += *a_ptr.add(i) * *b_ptr.add(i);
        }

        acc
    }
}

/// Computes the dot product of two float slices using x86 AVX2 + FMA intrinsics (256-bit registers).
///
/// Unrolled 4-way across 4 accumulator registers processing 32 `f32` values per iteration.
///
/// # Safety
///
/// The caller must ensure that the CPU supports AVX2 and FMA instructions before calling this function.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[inline]
pub unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    let mut a_ptr = a.as_ptr();
    let mut b_ptr = b.as_ptr();

    unsafe {
        let mut sum0 = _mm256_setzero_ps();
        let mut sum1 = _mm256_setzero_ps();
        let mut sum2 = _mm256_setzero_ps();
        let mut sum3 = _mm256_setzero_ps();

        let chunks32 = len / 32;
        for _ in 0..chunks32 {
            let va0 = _mm256_loadu_ps(a_ptr);
            let vb0 = _mm256_loadu_ps(b_ptr);
            sum0 = _mm256_fmadd_ps(va0, vb0, sum0);

            let va1 = _mm256_loadu_ps(a_ptr.add(8));
            let vb1 = _mm256_loadu_ps(b_ptr.add(8));
            sum1 = _mm256_fmadd_ps(va1, vb1, sum1);

            let va2 = _mm256_loadu_ps(a_ptr.add(16));
            let vb2 = _mm256_loadu_ps(b_ptr.add(16));
            sum2 = _mm256_fmadd_ps(va2, vb2, sum2);

            let va3 = _mm256_loadu_ps(a_ptr.add(24));
            let vb3 = _mm256_loadu_ps(b_ptr.add(24));
            sum3 = _mm256_fmadd_ps(va3, vb3, sum3);

            a_ptr = a_ptr.add(32);
            b_ptr = b_ptr.add(32);
        }

        let sum01 = _mm256_add_ps(sum0, sum1);
        let sum23 = _mm256_add_ps(sum2, sum3);
        let mut total_sum = _mm256_add_ps(sum01, sum23);

        // Process remaining 8-float chunks
        let rem = len % 32;
        let chunks8 = rem / 8;
        for _ in 0..chunks8 {
            let va = _mm256_loadu_ps(a_ptr);
            let vb = _mm256_loadu_ps(b_ptr);
            total_sum = _mm256_fmadd_ps(va, vb, total_sum);
            a_ptr = a_ptr.add(8);
            b_ptr = b_ptr.add(8);
        }

        // Horizontal reduce 256-bit register to scalar
        let sum128 = _mm_add_ps(
            _mm256_castps256_ps128(total_sum),
            _mm256_extractf128_ps(total_sum, 1),
        );
        let shuf = _mm_movehl_ps(sum128, sum128);
        let sums = _mm_add_ps(sum128, shuf);
        let shuf2 = _mm_shuffle_ps(sums, sums, 1);
        let mut acc = _mm_cvtss_f32(_mm_add_ss(sums, shuf2));

        // Process leftover scalars (0..7 floats)
        let tail = rem % 8;
        for i in 0..tail {
            acc += *a_ptr.add(i) * *b_ptr.add(i);
        }

        acc
    }
}

/// Computes the dot product using a clean portable scalar loop with compiler auto-vectorization.
#[inline]
pub fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    let mut sum = 0.0f32;
    for i in 0..len {
        sum += a[i] * b[i];
    }
    sum
}

/// Primary public entry point for dot product calculation with automatic hardware detection.
///
/// Dispatches to ARM NEON on aarch64, AVX2+FMA on x86_64 (via runtime feature detection),
/// or portable scalar fallback.
#[inline(always)]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    return unsafe { dot_product_neon(a, b) };

    #[cfg(target_arch = "x86_64")]
    return if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        unsafe { dot_product_avx2(a, b) }
    } else {
        dot_product_scalar(a, b)
    };

    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    return dot_product_scalar(a, b);
}

/// Computes the Euclidean $L_2$ norm ($\|v\|_2 = \sqrt{\sum v_i^2}$) of a vector.
#[inline]
pub fn l2_norm(v: &[f32]) -> f32 {
    dot_product(v, v).max(0.0).sqrt()
}

/// In-place normalizes a vector to unit length ($L_2$ norm = 1.0).
///
/// Returns the original $L_2$ norm before normalization. If the norm is zero or near-zero ($< 10^{-12}$),
/// the vector is left unchanged.
#[inline]
pub fn l2_normalize(v: &mut [f32]) -> f32 {
    let norm = l2_norm(v);
    if norm > 1e-12 {
        let inv_norm = 1.0 / norm;
        for x in v.iter_mut() {
            *x *= inv_norm;
        }
    }
    norm
}

/// Computes the cosine similarity between two pre-normalized unit vectors ($\|a\|_2 = 1, \|b\|_2 = 1$).
///
/// For pre-normalized embeddings (e.g. from `SemanticCache`), this is equivalent to a pure SIMD dot product.
#[inline]
pub fn cosine_similarity_normalized(a: &[f32], b: &[f32]) -> f32 {
    dot_product(a, b).clamp(-1.0, 1.0)
}

/// Computes the cosine similarity between two arbitrary vectors, normalizing on the fly.
///
/// $$\text{CosineSim}(a, b) = \frac{a \cdot b}{\|a\|_2 \|b\|_2}$$
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let norm_a = l2_norm(a);
    let norm_b = l2_norm(b);
    let denom = norm_a * norm_b;
    if denom > 1e-12 {
        (dot_product(a, b) / denom).clamp(-1.0, 1.0)
    } else {
        0.0
    }
}

/// Computes the squared Euclidean distance between two vectors: $\sum (a_i - b_i)^2$.
#[inline]
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    let mut sum = 0.0f32;
    for i in 0..len {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product_scalar_match() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = vec![2.0, 3.0, 4.0, 5.0, 6.0];
        // 1*2 + 2*3 + 3*4 + 4*5 + 5*6 = 2 + 6 + 12 + 20 + 30 = 70.0
        let expected = 70.0f32;
        assert_eq!(dot_product_scalar(&a, &b), expected);
        assert_eq!(dot_product(&a, &b), expected);
    }

    #[test]
    fn test_dot_product_various_dimensions() {
        // Test standard embedding dimensions: 64, 128, 384, 512, 768, 1024, 1536
        // plus odd dimension to test unrolling remainders
        for dim in [3, 7, 15, 16, 31, 32, 64, 128, 384, 512, 768, 1024, 1536] {
            let a: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01).collect();
            let b: Vec<f32> = (0..dim).map(|i| ((dim - i) as f32) * 0.02).collect();

            let scalar_res = dot_product_scalar(&a, &b);
            let simd_res = dot_product(&a, &b);

            let diff = (scalar_res - simd_res).abs();
            let rel_err = diff / scalar_res.abs().max(1.0);
            assert!(
                rel_err < 1e-4,
                "Dimension {} failed: scalar={}, simd={}, diff={}, rel_err={}",
                dim,
                scalar_res,
                simd_res,
                diff,
                rel_err
            );
        }
    }

    #[test]
    fn test_l2_norm_and_normalize() {
        let mut v = vec![3.0, 4.0];
        let norm = l2_normalize(&mut v);
        assert!((norm - 5.0).abs() < 1e-6);
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
        assert!((l2_norm(&v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];
        let d = vec![-1.0, 0.0, 0.0];

        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
        assert!((cosine_similarity(&a, &c) - 0.0).abs() < 1e-6);
        assert!((cosine_similarity(&a, &d) - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_zero_vector_similarity() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }
}
