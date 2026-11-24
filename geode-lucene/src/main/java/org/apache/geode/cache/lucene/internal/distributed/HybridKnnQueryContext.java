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

package org.apache.geode.cache.lucene.internal.distributed;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Serializable argument payload for {@link DistributedHybridKnnFunction}, carrying the k-NN query
 * vector, similarity function, optional Lucene/metadata filter predicates, and Tier-0 delta buffer
 * toggle.
 */
public class HybridKnnQueryContext implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final int DEFAULT_RERANK_OVERSAMPLE_FACTOR = 1;
  public static final int MAX_RERANK_OVERSAMPLE_FACTOR = 64;

  private final String indexName;
  private final String vectorFieldName;
  private final float[] queryVector;
  private final int k;
  private final String similarityFunctionName;
  private final String filterQueryString;
  private final String defaultFilterField;
  private final HashMap<String, String> metadataEqualityFilters;
  private final boolean includeTier0;
  private final int rerankOversampleFactor;

  private HybridKnnQueryContext(Builder builder) {
    this.indexName = builder.indexName;
    this.vectorFieldName = builder.vectorFieldName;
    this.queryVector = Arrays.copyOf(builder.queryVector, builder.queryVector.length);
    this.k = builder.k;
    this.similarityFunctionName = builder.similarityFunction.name();
    this.filterQueryString = builder.filterQueryString;
    this.defaultFilterField = builder.defaultFilterField;
    this.metadataEqualityFilters = new HashMap<>(builder.metadataEqualityFilters);
    this.includeTier0 = builder.includeTier0;
    this.rerankOversampleFactor = builder.rerankOversampleFactor;
  }

  public static Builder builder(String indexName, String vectorFieldName, float[] queryVector,
      int k) {
    return new Builder(indexName, vectorFieldName, queryVector, k);
  }

  public String getIndexName() {
    return indexName;
  }

  public String getVectorFieldName() {
    return vectorFieldName;
  }

  public float[] getQueryVector() {
    return Arrays.copyOf(queryVector, queryVector.length);
  }

  public int getK() {
    return k;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return VectorSimilarityFunction.valueOf(similarityFunctionName);
  }

  public String getFilterQueryString() {
    return filterQueryString;
  }

  public String getDefaultFilterField() {
    return defaultFilterField;
  }

  public Map<String, String> getMetadataEqualityFilters() {
    return metadataEqualityFilters == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(metadataEqualityFilters);
  }

  public boolean isIncludeTier0() {
    return includeTier0;
  }

  /**
   * Oversampling multiplier for optional exact FP32 {@code .vec} reranking on SQ8-quantized
   * indexes. A value of {@code 1} (default) disables FP32 reranking; values in {@code [2, 64]}
   * fetch {@code k * rerankOversampleFactor} candidates from HNSW and rescore them with exact
   * stored FP32 vectors before returning top-{@code k}.
   */
  public int getRerankOversampleFactor() {
    return rerankOversampleFactor <= 0 ? DEFAULT_RERANK_OVERSAMPLE_FACTOR : rerankOversampleFactor;
  }

  public static final class Builder {
    private final String indexName;
    private final String vectorFieldName;
    private final float[] queryVector;
    private final int k;
    private VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
    private String filterQueryString;
    private String defaultFilterField;
    private final HashMap<String, String> metadataEqualityFilters = new HashMap<>();
    private boolean includeTier0 = true;
    private int rerankOversampleFactor = DEFAULT_RERANK_OVERSAMPLE_FACTOR;

    private Builder(String indexName, String vectorFieldName, float[] queryVector, int k) {
      this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
      this.vectorFieldName =
          Objects.requireNonNull(vectorFieldName, "vectorFieldName must not be null");
      Objects.requireNonNull(queryVector, "queryVector must not be null");
      if (queryVector.length == 0) {
        throw new IllegalArgumentException("queryVector must not be empty");
      }
      if (k <= 0) {
        throw new IllegalArgumentException("k must be positive, got: " + k);
      }
      this.queryVector = Arrays.copyOf(queryVector, queryVector.length);
      this.k = k;
    }

    public Builder similarityFunction(VectorSimilarityFunction similarityFunction) {
      this.similarityFunction =
          Objects.requireNonNull(similarityFunction, "similarityFunction must not be null");
      return this;
    }

    public Builder filterQuery(String filterQueryString, String defaultFilterField) {
      this.filterQueryString = filterQueryString;
      this.defaultFilterField = defaultFilterField;
      return this;
    }

    public Builder addMetadataFilter(String field, String value) {
      Objects.requireNonNull(field, "filter field must not be null");
      Objects.requireNonNull(value, "filter value must not be null");
      this.metadataEqualityFilters.put(field, value);
      return this;
    }

    public Builder metadataFilters(Map<String, String> filters) {
      if (filters != null) {
        filters.forEach(this::addMetadataFilter);
      }
      return this;
    }

    public Builder includeTier0(boolean includeTier0) {
      this.includeTier0 = includeTier0;
      return this;
    }

    public Builder rerankOversampleFactor(int rerankOversampleFactor) {
      if (rerankOversampleFactor < DEFAULT_RERANK_OVERSAMPLE_FACTOR
          || rerankOversampleFactor > MAX_RERANK_OVERSAMPLE_FACTOR) {
        throw new IllegalArgumentException(
            "rerankOversampleFactor must be in [" + DEFAULT_RERANK_OVERSAMPLE_FACTOR + ", "
                + MAX_RERANK_OVERSAMPLE_FACTOR + "], got: " + rerankOversampleFactor);
      }
      this.rerankOversampleFactor = rerankOversampleFactor;
      return this;
    }

    public HybridKnnQueryContext build() {
      return new HybridKnnQueryContext(this);
    }
  }
}
