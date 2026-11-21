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
package org.apache.geode.cache.lucene;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.VectorSimilarityFunction;

import org.apache.geode.DataSerializer;

/**
 * Immutable descriptor for a vector field configured on a {@link LuceneIndex}.
 *
 * <p>
 * Holds the field name, vector dimensionality, {@link VectorSimilarityFunction} metric, HNSW graph
 * construction parameters ({@code maxConnections} M and {@code beamWidth} efConstruction), and
 * whether 8-bit scalar quantization (SQ8) is enabled.
 *
 * @since Geode 2.1
 */
public final class VectorIndexMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Default HNSW maximum connections per node (M). */
  public static final int DEFAULT_MAX_CONNECTIONS = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;

  /** Default HNSW construction beam width (efConstruction). */
  public static final int DEFAULT_BEAM_WIDTH = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;

  /** Maximum allowed HNSW connections per node (M) in Lucene 9.12. */
  public static final int MAX_CONNECTIONS_LIMIT = Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;

  /** Maximum allowed HNSW construction beam width (efConstruction) in Lucene 9.12. */
  public static final int MAX_BEAM_WIDTH_LIMIT = Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;

  private final String fieldName;
  private final int dimension;
  private final VectorSimilarityFunction similarityFunction;
  private final int maxConnections;
  private final int beamWidth;
  private final boolean quantizeSQ8;

  /**
   * Constructs a {@code VectorIndexMetadata} descriptor using default HNSW graph parameters
   * ({@link #DEFAULT_MAX_CONNECTIONS} and {@link #DEFAULT_BEAM_WIDTH}).
   *
   * @param fieldName name of the vector field (must not be null or blank)
   * @param dimension vector dimensionality (must be &gt; 0)
   * @param similarityFunction Lucene similarity function (must not be null)
   * @param quantizeSQ8 whether scalar quantization (SQ8) is enabled for this vector field
   * @throws IllegalArgumentException if any parameter violates validation constraints
   */
  public VectorIndexMetadata(String fieldName, int dimension,
      VectorSimilarityFunction similarityFunction, boolean quantizeSQ8) {
    this(fieldName, dimension, similarityFunction, DEFAULT_MAX_CONNECTIONS, DEFAULT_BEAM_WIDTH,
        quantizeSQ8);
  }

  /**
   * Constructs a {@code VectorIndexMetadata} descriptor with explicit HNSW graph construction
   * parameters.
   *
   * @param fieldName name of the vector field (must not be null or blank)
   * @param dimension vector dimensionality (must be &gt; 0)
   * @param similarityFunction Lucene similarity function (must not be null)
   * @param maxConnections HNSW maximum connections per node M (must be in [1, 512])
   * @param beamWidth HNSW construction beam width efConstruction (must be in [1, 3200])
   * @param quantizeSQ8 whether scalar quantization (SQ8) is enabled for this vector field
   * @throws IllegalArgumentException if any parameter violates validation constraints
   */
  public VectorIndexMetadata(String fieldName, int dimension,
      VectorSimilarityFunction similarityFunction, int maxConnections, int beamWidth,
      boolean quantizeSQ8) {
    validate(fieldName, dimension, similarityFunction, maxConnections, beamWidth);
    this.fieldName = fieldName;
    this.dimension = dimension;
    this.similarityFunction = similarityFunction;
    this.maxConnections = maxConnections;
    this.beamWidth = beamWidth;
    this.quantizeSQ8 = quantizeSQ8;
  }

  private static void validate(String fieldName, int dimension,
      VectorSimilarityFunction similarityFunction, int maxConnections, int beamWidth) {
    if (fieldName == null || fieldName.trim().isEmpty()) {
      throw new IllegalArgumentException("Vector field name must not be null or empty");
    }
    if (dimension <= 0) {
      throw new IllegalArgumentException(
          "Vector dimension must be positive, got: " + dimension);
    }
    if (similarityFunction == null) {
      throw new IllegalArgumentException("Vector similarity function must not be null");
    }
    if (maxConnections <= 0 || maxConnections > MAX_CONNECTIONS_LIMIT) {
      throw new IllegalArgumentException(
          "HNSW maxConnections (M) must be between 1 and " + MAX_CONNECTIONS_LIMIT + ", got: "
              + maxConnections);
    }
    if (beamWidth <= 0 || beamWidth > MAX_BEAM_WIDTH_LIMIT) {
      throw new IllegalArgumentException(
          "HNSW beamWidth (efConstruction) must be between 1 and " + MAX_BEAM_WIDTH_LIMIT
              + ", got: " + beamWidth);
    }
  }

  public String getFieldName() {
    return fieldName;
  }

  public int getDimension() {
    return dimension;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getBeamWidth() {
    return beamWidth;
  }

  public boolean isQuantizeSQ8() {
    return quantizeSQ8;
  }

  /**
   * Serializes this descriptor to a Geode {@link DataOutput} stream.
   */
  public void writeTo(DataOutput out) throws IOException {
    DataSerializer.writeString(fieldName, out);
    out.writeInt(dimension);
    DataSerializer.writeString(similarityFunction.name(), out);
    out.writeInt(maxConnections);
    out.writeInt(beamWidth);
    out.writeBoolean(quantizeSQ8);
  }

  /**
   * Deserializes a {@code VectorIndexMetadata} descriptor from a Geode {@link DataInput} stream.
   */
  public static VectorIndexMetadata readFrom(DataInput in) throws IOException {
    String fieldName = DataSerializer.readString(in);
    int dimension = in.readInt();
    String similarityName = DataSerializer.readString(in);
    VectorSimilarityFunction similarityFunction;
    try {
      similarityFunction = VectorSimilarityFunction.valueOf(similarityName);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("Unknown VectorSimilarityFunction: " + similarityName, e);
    }
    int maxConnections = in.readInt();
    int beamWidth = in.readInt();
    boolean quantizeSQ8 = in.readBoolean();
    return new VectorIndexMetadata(fieldName, dimension, similarityFunction, maxConnections,
        beamWidth, quantizeSQ8);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorIndexMetadata)) {
      return false;
    }
    VectorIndexMetadata that = (VectorIndexMetadata) o;
    return dimension == that.dimension
        && maxConnections == that.maxConnections
        && beamWidth == that.beamWidth
        && quantizeSQ8 == that.quantizeSQ8
        && fieldName.equals(that.fieldName)
        && similarityFunction == that.similarityFunction;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, dimension, similarityFunction, maxConnections, beamWidth,
        quantizeSQ8);
  }

  @Override
  public String toString() {
    return "VectorIndexMetadata{"
        + "fieldName='" + fieldName + '\''
        + ", dimension=" + dimension
        + ", similarityFunction=" + similarityFunction
        + ", maxConnections=" + maxConnections
        + ", beamWidth=" + beamWidth
        + ", quantizeSQ8=" + quantizeSQ8
        + '}';
  }
}
