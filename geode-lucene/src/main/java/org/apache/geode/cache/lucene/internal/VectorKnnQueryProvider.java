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
package org.apache.geode.cache.lucene.internal;

import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.repository.serializer.VectorFieldMapper;

/**
 * {@link LuceneQueryProvider} constructing a Lucene 9.12 {@link KnnFloatVectorQuery} with optional
 * pre-filter predicate parsed via {@link StandardQueryParser}.
 */
public class VectorKnnQueryProvider implements LuceneQueryProvider {
  private static final long serialVersionUID = 1L;

  private final String vectorField;
  private final float[] queryVector;
  private final int k;
  private final String filterQuery;
  private final String defaultField;

  public VectorKnnQueryProvider(String vectorField, float[] queryVector, int k, String filterQuery,
      String defaultField) {
    if (queryVector == null || queryVector.length == 0) {
      throw new IllegalArgumentException("Query vector must not be null or empty");
    }
    if (k <= 0) {
      throw new IllegalArgumentException("k must be positive, got: " + k);
    }
    this.vectorField = vectorField;
    this.queryVector = Arrays.copyOf(queryVector, queryVector.length);
    this.k = k;
    this.filterQuery = filterQuery;
    this.defaultField = defaultField;
  }

  public String getVectorField() {
    return vectorField;
  }

  public float[] getQueryVector() {
    return queryVector == null ? null : Arrays.copyOf(queryVector, queryVector.length);
  }

  public int getK() {
    return k;
  }

  public String getFilterQuery() {
    return filterQuery;
  }

  public String getDefaultField() {
    return defaultField;
  }

  @Override
  public Query getQuery(LuceneIndex index) throws LuceneQueryException {
    if (index == null) {
      throw new LuceneQueryException("LuceneIndex must not be null");
    }
    VectorIndexMetadata metadata = resolveVectorFieldMetadata(index);
    try {
      VectorFieldMapper.validateVector(queryVector, metadata);
    } catch (IllegalArgumentException e) {
      throw new LuceneQueryException(e.getMessage(), e);
    }

    if (filterQuery != null && !filterQuery.trim().isEmpty()) {
      Query parsedFilter = parseFilterQuery(index, filterQuery);
      return new KnnFloatVectorQuery(metadata.getFieldName(), queryVector, k, parsedFilter);
    }
    return new KnnFloatVectorQuery(metadata.getFieldName(), queryVector, k);
  }

  private VectorIndexMetadata resolveVectorFieldMetadata(LuceneIndex index)
      throws LuceneQueryException {
    Map<String, VectorIndexMetadata> vectorFields = index.getVectorFields();
    if (vectorFields == null || vectorFields.isEmpty()) {
      throw new LuceneQueryException(
          "Lucene index '" + index.getName() + "' has no vector fields configured");
    }
    if (vectorField != null && !vectorField.trim().isEmpty()) {
      VectorIndexMetadata metadata = vectorFields.get(vectorField.trim());
      if (metadata == null) {
        throw new LuceneQueryException(
            "Vector field '" + vectorField + "' is not configured on Lucene index '"
                + index.getName() + "'. Available vector fields: " + vectorFields.keySet());
      }
      return metadata;
    }
    if (vectorFields.size() == 1) {
      return vectorFields.values().iterator().next();
    }
    throw new LuceneQueryException(
        "Multiple vector fields configured on Lucene index '" + index.getName() + "' "
            + vectorFields.keySet() + "; specify --vector-field");
  }

  private Query parseFilterQuery(LuceneIndex index, String rawFilter) throws LuceneQueryException {
    Analyzer indexAnalyzer = index instanceof LuceneIndexImpl
        ? ((LuceneIndexImpl) index).getAnalyzer()
        : null;
    String effectiveDefaultField = defaultField;
    if (effectiveDefaultField == null || effectiveDefaultField.trim().isEmpty()) {
      String[] fieldNames = index.getFieldNames();
      effectiveDefaultField =
          (fieldNames != null && fieldNames.length > 0) ? fieldNames[0] : null;
    }

    try (
        StandardAnalyzer fallbackAnalyzer = indexAnalyzer == null ? new StandardAnalyzer() : null) {
      Analyzer analyzer = indexAnalyzer != null ? indexAnalyzer : fallbackAnalyzer;
      StandardQueryParser parser = new StandardQueryParser(analyzer);
      parser.setAllowLeadingWildcard(true);
      Query parsed =
          parser.parse(rawFilter, effectiveDefaultField == null ? "" : effectiveDefaultField);
      if (effectiveDefaultField == null && referencesEmptyField(parsed)) {
        throw new LuceneQueryException(
            "Filter query '" + rawFilter + "' contains unqualified terms on pure-vector index '"
                + index.getName()
                + "'; qualify fields explicitly (e.g. field:value) or specify --defaultField.");
      }
      return parsed;
    } catch (QueryNodeException e) {
      throw new LuceneQueryException(
          String.format("Parsing filter query '%s' failed due to: %s", rawFilter, e.getMessage()),
          e);
    }
  }

  private static boolean referencesEmptyField(Query query) {
    final boolean[] foundEmpty = new boolean[1];
    query.visit(new QueryVisitor() {
      @Override
      public boolean acceptField(String field) {
        if (field == null || field.isEmpty()) {
          foundEmpty[0] = true;
        }
        return true;
      }

      @Override
      public void consumeTerms(Query q, Term... terms) {
        for (Term term : terms) {
          if (term.field() == null || term.field().isEmpty()) {
            foundEmpty[0] = true;
          }
        }
      }
    });
    return foundEmpty[0];
  }
}
