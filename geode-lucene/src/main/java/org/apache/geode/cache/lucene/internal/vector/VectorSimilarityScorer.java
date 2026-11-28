/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.lucene.internal.vector;

import java.util.Objects;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * SIMD-friendly unrolled exact vector similarity scorer whose score semantics match Apache Lucene
 * 9.12.x {@link VectorSimilarityFunction#compare(float[], float[])}, allowing Tier-0 delta buffer
 * scores to be merged directly with Lucene HNSW {@code TopDocs} scores.
 */
public final class VectorSimilarityScorer {

  private VectorSimilarityScorer() {}

  /**
   * Computes the Lucene-compatible similarity score between {@code query} and {@code candidate}.
   *
   * @param query query embedding vector
   * @param candidate candidate embedding vector
   * @param similarity Lucene similarity metric
   * @return non-negative Lucene-compatible similarity score
   */
  public static float score(float[] query, float[] candidate, VectorSimilarityFunction similarity) {
    Objects.requireNonNull(query, "query must not be null");
    Objects.requireNonNull(candidate, "candidate must not be null");
    Objects.requireNonNull(similarity, "similarity must not be null");
    validateDimensions(query, candidate);

    switch (similarity) {
      case COSINE:
        float qNormSq = dotProductUnrolled(query, query);
        return cosineScore(query, qNormSq, candidate);
      case DOT_PRODUCT:
        return dotProductScore(query, candidate);
      case EUCLIDEAN:
        return euclideanScore(query, candidate);
      case MAXIMUM_INNER_PRODUCT:
        return maxInnerProductScore(query, candidate);
      default:
        return similarity.compare(query, candidate);
    }
  }

  /**
   * Computes Lucene-compatible similarity scores for a contiguous batch of candidate vectors,
   * reusing precomputed query norms for {@link VectorSimilarityFunction#COSINE}.
   *
   * @param query query embedding vector
   * @param candidates candidate vectors array (first {@code count} entries are scored)
   * @param count number of candidates to score
   * @param similarity Lucene similarity metric
   * @param scoresOut output buffer of length {@code >= count}
   */
  public static void scoreBatch(float[] query, float[][] candidates, int count,
      VectorSimilarityFunction similarity, float[] scoresOut) {
    Objects.requireNonNull(query, "query must not be null");
    Objects.requireNonNull(candidates, "candidates must not be null");
    Objects.requireNonNull(similarity, "similarity must not be null");
    Objects.requireNonNull(scoresOut, "scoresOut must not be null");
    if (count < 0 || count > candidates.length || count > scoresOut.length) {
      throw new IllegalArgumentException(
          "Invalid batch count " + count + " for candidates=" + candidates.length
              + ", scoresOut=" + scoresOut.length);
    }
    if (count == 0) {
      return;
    }

    switch (similarity) {
      case COSINE: {
        float queryNormSq = dotProductUnrolled(query, query);
        for (int i = 0; i < count; i++) {
          float[] candidate = candidates[i];
          validateDimensions(query, candidate);
          scoresOut[i] = cosineScore(query, queryNormSq, candidate);
        }
        break;
      }
      case DOT_PRODUCT: {
        for (int i = 0; i < count; i++) {
          float[] candidate = candidates[i];
          validateDimensions(query, candidate);
          scoresOut[i] = dotProductScore(query, candidate);
        }
        break;
      }
      case EUCLIDEAN: {
        for (int i = 0; i < count; i++) {
          float[] candidate = candidates[i];
          validateDimensions(query, candidate);
          scoresOut[i] = euclideanScore(query, candidate);
        }
        break;
      }
      case MAXIMUM_INNER_PRODUCT: {
        for (int i = 0; i < count; i++) {
          float[] candidate = candidates[i];
          validateDimensions(query, candidate);
          scoresOut[i] = maxInnerProductScore(query, candidate);
        }
        break;
      }
      default: {
        for (int i = 0; i < count; i++) {
          float[] candidate = candidates[i];
          validateDimensions(query, candidate);
          scoresOut[i] = similarity.compare(query, candidate);
        }
        break;
      }
    }
  }

  /**
   * 8-way unrolled inner product designed for JIT SuperWord auto-vectorization.
   */
  public static float dotProductUnrolled(float[] a, float[] b) {
    final int len = a.length;
    float s0 = 0.0f;
    float s1 = 0.0f;
    float s2 = 0.0f;
    float s3 = 0.0f;
    float s4 = 0.0f;
    float s5 = 0.0f;
    float s6 = 0.0f;
    float s7 = 0.0f;

    int i = 0;
    final int limit8 = len & ~7;
    for (; i < limit8; i += 8) {
      s0 = Math.fma(a[i], b[i], s0);
      s1 = Math.fma(a[i + 1], b[i + 1], s1);
      s2 = Math.fma(a[i + 2], b[i + 2], s2);
      s3 = Math.fma(a[i + 3], b[i + 3], s3);
      s4 = Math.fma(a[i + 4], b[i + 4], s4);
      s5 = Math.fma(a[i + 5], b[i + 5], s5);
      s6 = Math.fma(a[i + 6], b[i + 6], s6);
      s7 = Math.fma(a[i + 7], b[i + 7], s7);
    }

    float sum = ((s0 + s1) + (s2 + s3)) + ((s4 + s5) + (s6 + s7));
    for (; i < len; i++) {
      sum = Math.fma(a[i], b[i], sum);
    }
    return sum;
  }

  /**
   * 8-way unrolled squared Euclidean distance designed for JIT SuperWord auto-vectorization.
   */
  public static float squaredDistanceUnrolled(float[] a, float[] b) {
    final int len = a.length;
    float s0 = 0.0f;
    float s1 = 0.0f;
    float s2 = 0.0f;
    float s3 = 0.0f;
    float s4 = 0.0f;
    float s5 = 0.0f;
    float s6 = 0.0f;
    float s7 = 0.0f;

    int i = 0;
    final int limit8 = len & ~7;
    for (; i < limit8; i += 8) {
      float d0 = a[i] - b[i];
      float d1 = a[i + 1] - b[i + 1];
      float d2 = a[i + 2] - b[i + 2];
      float d3 = a[i + 3] - b[i + 3];
      float d4 = a[i + 4] - b[i + 4];
      float d5 = a[i + 5] - b[i + 5];
      float d6 = a[i + 6] - b[i + 6];
      float d7 = a[i + 7] - b[i + 7];
      s0 = Math.fma(d0, d0, s0);
      s1 = Math.fma(d1, d1, s1);
      s2 = Math.fma(d2, d2, s2);
      s3 = Math.fma(d3, d3, s3);
      s4 = Math.fma(d4, d4, s4);
      s5 = Math.fma(d5, d5, s5);
      s6 = Math.fma(d6, d6, s6);
      s7 = Math.fma(d7, d7, s7);
    }

    float sum = ((s0 + s1) + (s2 + s3)) + ((s4 + s5) + (s6 + s7));
    for (; i < len; i++) {
      float d = a[i] - b[i];
      sum = Math.fma(d, d, sum);
    }
    return sum;
  }

  private static float cosineScore(float[] query, float queryNormSq, float[] candidate) {
    final int len = query.length;
    float dot0 = 0.0f;
    float dot1 = 0.0f;
    float dot2 = 0.0f;
    float dot3 = 0.0f;
    float norm0 = 0.0f;
    float norm1 = 0.0f;
    float norm2 = 0.0f;
    float norm3 = 0.0f;

    int i = 0;
    final int limit4 = len & ~3;
    for (; i < limit4; i += 4) {
      float c0 = candidate[i];
      float c1 = candidate[i + 1];
      float c2 = candidate[i + 2];
      float c3 = candidate[i + 3];
      dot0 = Math.fma(query[i], c0, dot0);
      dot1 = Math.fma(query[i + 1], c1, dot1);
      dot2 = Math.fma(query[i + 2], c2, dot2);
      dot3 = Math.fma(query[i + 3], c3, dot3);
      norm0 = Math.fma(c0, c0, norm0);
      norm1 = Math.fma(c1, c1, norm1);
      norm2 = Math.fma(c2, c2, norm2);
      norm3 = Math.fma(c3, c3, norm3);
    }

    float dot = (dot0 + dot1) + (dot2 + dot3);
    float candNormSq = (norm0 + norm1) + (norm2 + norm3);
    for (; i < len; i++) {
      float c = candidate[i];
      dot = Math.fma(query[i], c, dot);
      candNormSq = Math.fma(c, c, candNormSq);
    }

    double denom = Math.sqrt((double) queryNormSq * (double) candNormSq);
    if (denom == 0.0d) {
      return 0.0f;
    }
    float cos = (float) (dot / denom);
    return Math.max((1.0f + cos) / 2.0f, 0.0f);
  }

  private static float dotProductScore(float[] query, float[] candidate) {
    float dot = dotProductUnrolled(query, candidate);
    return Math.max((1.0f + dot) / 2.0f, 0.0f);
  }

  private static float euclideanScore(float[] query, float[] candidate) {
    float sqDist = squaredDistanceUnrolled(query, candidate);
    return 1.0f / (1.0f + sqDist);
  }

  private static float maxInnerProductScore(float[] query, float[] candidate) {
    float dot = dotProductUnrolled(query, candidate);
    if (dot < 0.0f) {
      return 1.0f / (1.0f - dot);
    }
    return dot + 1.0f;
  }

  private static void validateDimensions(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch: query=" + a.length + ", candidate=" + b.length);
    }
  }
}
