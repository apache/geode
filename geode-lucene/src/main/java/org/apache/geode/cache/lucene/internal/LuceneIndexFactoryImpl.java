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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.VectorSimilarityFunction;

import org.apache.geode.cache.lucene.LuceneIndexFactory;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.VectorIndexMetadata;

public class LuceneIndexFactoryImpl implements LuceneIndexFactory {
  private final LuceneServiceImpl service;
  private final Map<String, Analyzer> fields = new LinkedHashMap<>();
  private final Map<String, VectorIndexMetadata> vectorFields = new LinkedHashMap<>();
  private LuceneSerializer serializer;


  public LuceneIndexFactoryImpl(final LuceneServiceImpl luceneService) {
    service = luceneService;
  }

  @Override
  public LuceneIndexFactoryImpl addField(final String name) {
    return addField(name, new StandardAnalyzer());
  }

  @Override
  public LuceneIndexFactoryImpl setFields(final String... fields) {
    this.fields.clear();
    for (String field : fields) {
      addField(field);
    }
    return this;
  }

  @Override
  public LuceneIndexFactoryImpl addField(final String name, final Analyzer analyzer) {
    if (vectorFields.containsKey(name)) {
      throw new IllegalArgumentException(
          "Field '" + name + "' cannot be configured as both a text field and a vector field");
    }
    fields.put(name, analyzer);
    return this;
  }

  @Override
  public LuceneIndexFactoryImpl setFields(final Map<String, Analyzer> fieldMap) {
    if (fieldMap != null) {
      for (String name : fieldMap.keySet()) {
        if (vectorFields.containsKey(name)) {
          throw new IllegalArgumentException(
              "Field '" + name + "' cannot be configured as both a text field and a vector field");
        }
      }
    }
    fields.clear();
    if (fieldMap != null) {
      fields.putAll(fieldMap);
    }
    return this;
  }

  @Override
  public void create(final String indexName, final String regionPath) {
    create(indexName, regionPath, false);
  }

  public void create(final String indexName, final String regionPath,
      boolean allowOnExistingRegion) {
    if (vectorFields.isEmpty()) {
      service.createIndex(indexName, regionPath, fields, serializer, allowOnExistingRegion);
    } else {
      service.createIndex(indexName, regionPath, fields, vectorFields, serializer,
          allowOnExistingRegion);
    }
  }

  @Override
  public LuceneIndexFactoryImpl setLuceneSerializer(LuceneSerializer luceneSerializer) {
    serializer = luceneSerializer;
    return this;
  }

  @Override
  public LuceneIndexFactoryImpl addVectorField(final String name, final int dimension,
      final VectorSimilarityFunction similarity, final boolean sq8) {
    return addVectorField(new VectorIndexMetadata(name, dimension, similarity, sq8));
  }

  @Override
  public LuceneIndexFactoryImpl addVectorField(final String name, final int dimension,
      final VectorSimilarityFunction similarity, final int maxConnections, final int beamWidth,
      final boolean sq8) {
    return addVectorField(
        new VectorIndexMetadata(name, dimension, similarity, maxConnections, beamWidth, sq8));
  }

  @Override
  public LuceneIndexFactoryImpl addVectorField(final VectorIndexMetadata metadata) {
    if (metadata == null) {
      throw new IllegalArgumentException("VectorIndexMetadata must not be null");
    }
    String fieldName = metadata.getFieldName();
    if (vectorFields.containsKey(fieldName)) {
      throw new IllegalArgumentException("Duplicate vector field name: '" + fieldName + "'");
    }
    if (fields.containsKey(fieldName)) {
      throw new IllegalArgumentException(
          "Field '" + fieldName + "' cannot be configured as both a text field and a vector field");
    }
    vectorFields.put(fieldName, metadata);
    return this;
  }

  public Map<String, VectorIndexMetadata> getVectorFields() {
    return Collections.unmodifiableMap(vectorFields);
  }
}
