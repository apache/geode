/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.vector.continuous;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable continuous vector subscription {@code (q_m, tau_m)} where {@code q_m} is L2-normalized
 * onto the unit hypersphere {@code S^{d-1}} and {@code tau_m in (-1, 1]} is the minimum cosine
 * similarity threshold.
 */
public final class VectorSubscription implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String subscriptionId;
  private final float[] normalizedQueryVector;
  private final float similarityThreshold;
  private final double chordalTolerance;

  public VectorSubscription(String subscriptionId, float[] queryVector, float similarityThreshold) {
    if (subscriptionId == null || subscriptionId.trim().isEmpty()) {
      throw new IllegalArgumentException("subscriptionId must not be null or empty");
    }
    if (Float.isNaN(similarityThreshold) || similarityThreshold <= -1.0f
        || similarityThreshold > 1.0f) {
      throw new IllegalArgumentException(
          "similarityThreshold must be in (-1, 1], got: " + similarityThreshold);
    }
    this.subscriptionId = subscriptionId;
    this.normalizedQueryVector = normalize(queryVector);
    this.similarityThreshold = similarityThreshold;
    this.chordalTolerance = Math.sqrt(Math.max(0.0d, 2.0d * (1.0d - similarityThreshold)));
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  /**
   * Returns a defensive copy of the L2-normalized query vector on {@code S^{d-1}}.
   */
  public float[] getNormalizedQueryVector() {
    return normalizedQueryVector.clone();
  }

  float[] getNormalizedQueryVectorUnsafe() {
    return normalizedQueryVector;
  }

  public int getDimension() {
    return normalizedQueryVector.length;
  }

  public float getSimilarityThreshold() {
    return similarityThreshold;
  }

  /**
   * Returns the chordal tolerance {@code rho_m = sqrt(2 * (1 - tau_m))}.
   */
  public double getChordalTolerance() {
    return chordalTolerance;
  }

  /**
   * L2-normalizes a float vector onto the unit hypersphere {@code S^{d-1}}.
   */
  public static float[] normalize(float[] vector) {
    if (vector == null || vector.length == 0) {
      throw new IllegalArgumentException("Vector must not be null or empty");
    }
    double normSq = 0.0d;
    for (float v : vector) {
      if (!Float.isFinite(v)) {
        throw new IllegalArgumentException("Vector contains non-finite value: " + v);
      }
      normSq += (double) v * (double) v;
    }
    if (normSq == 0.0d) {
      throw new IllegalArgumentException("Vector must have non-zero L2 norm");
    }
    double invNorm = 1.0d / Math.sqrt(normSq);
    float[] unit = new float[vector.length];
    for (int i = 0; i < vector.length; i++) {
      unit[i] = (float) (vector[i] * invNorm);
    }
    return unit;
  }

  /**
   * Computes an 8-way unrolled SIMD-friendly dot product between {@code query} and a contiguous row
   * starting at {@code matrixOffset} in {@code matrix}.
   */
  public static float dotProductContiguous(float[] query, float[] matrix, int matrixOffset,
      int dim) {
    float s0 = 0.0f;
    float s1 = 0.0f;
    float s2 = 0.0f;
    float s3 = 0.0f;
    float s4 = 0.0f;
    float s5 = 0.0f;
    float s6 = 0.0f;
    float s7 = 0.0f;

    int i = 0;
    int limit = dim & ~7;
    for (; i < limit; i += 8) {
      int m = matrixOffset + i;
      s0 = Math.fma(query[i], matrix[m], s0);
      s1 = Math.fma(query[i + 1], matrix[m + 1], s1);
      s2 = Math.fma(query[i + 2], matrix[m + 2], s2);
      s3 = Math.fma(query[i + 3], matrix[m + 3], s3);
      s4 = Math.fma(query[i + 4], matrix[m + 4], s4);
      s5 = Math.fma(query[i + 5], matrix[m + 5], s5);
      s6 = Math.fma(query[i + 6], matrix[m + 6], s6);
      s7 = Math.fma(query[i + 7], matrix[m + 7], s7);
    }
    float sum = ((s0 + s1) + (s2 + s3)) + ((s4 + s5) + (s6 + s7));
    for (; i < dim; i++) {
      sum = Math.fma(query[i], matrix[matrixOffset + i], sum);
    }
    return sum;
  }

  /**
   * Computes exact double-precision Euclidean chordal distance {@code ||a - b||_2} on
   * {@code S^{d-1}}.
   */
  public static double euclideanChordalDistance(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimensions must match: " + a.length + " vs " + b.length);
    }
    double sumSq = 0.0d;
    for (int i = 0; i < a.length; i++) {
      double diff = (double) a[i] - (double) b[i];
      sumSq += diff * diff;
    }
    return Math.sqrt(sumSq);
  }

  /**
   * Computes chordal distance {@code sqrt(max(0, 2 * (1 - <a, b>)))} from cosine similarity.
   */
  public static double chordalDistanceFromCosine(double cosineSimilarity) {
    double clamped = Math.max(-1.0d, Math.min(1.0d, cosineSimilarity));
    return Math.sqrt(Math.max(0.0d, 2.0d * (1.0d - clamped)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorSubscription)) {
      return false;
    }
    VectorSubscription that = (VectorSubscription) o;
    return Float.compare(that.similarityThreshold, similarityThreshold) == 0
        && Objects.equals(subscriptionId, that.subscriptionId)
        && Arrays.equals(normalizedQueryVector, that.normalizedQueryVector);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(subscriptionId, similarityThreshold);
    result = 31 * result + Arrays.hashCode(normalizedQueryVector);
    return result;
  }

  @Override
  public String toString() {
    return "VectorSubscription{"
        + "subscriptionId='" + subscriptionId + '\''
        + ", dimension=" + normalizedQueryVector.length
        + ", similarityThreshold=" + similarityThreshold
        + '}';
  }
}
