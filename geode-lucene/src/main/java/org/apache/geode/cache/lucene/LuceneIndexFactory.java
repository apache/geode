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

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

/**
 * A factory for creating a lucene index on the current member. Obtain a factory from
 * {@link LuceneService#createIndexFactory()}.
 *
 * Configure the index using the add methods, and then call {@link #create(String, String)} to
 * create the index.
 */
public interface LuceneIndexFactory {

  /**
   * Add a field to be indexed
   *
   * @param name A field of the object to index. Only fields listed here will be stored in the
   *        index. Fields should map to PDX fieldNames if the object is serialized with PDX, or to
   *        java fields on the object otherwise. The special field name
   *        {@link LuceneService#REGION_VALUE_FIELD} indicates that the entire value should be
   *        stored as a single field in the index.
   */
  LuceneIndexFactory addField(String name);

  /**
   * Set the list of fields to be indexed.
   *
   * @param fields Fields of the object to index. Only fields listed here will be stored in the
   *        index. Fields should map to PDX fieldNames if the object is serialized with PDX, or to
   *        java fields on the object otherwise. The special field name
   *        {@link LuceneService#REGION_VALUE_FIELD} indicates that the entire value should be
   *        stored as a single field in the index.
   */
  LuceneIndexFactory setFields(String... fields);

  /**
   * Add a field to be indexed, using the specified analyzer.
   *
   * @param name A field of the object to index. Only fields listed here will be stored in the
   *        index. Fields should map to PDX fieldNames if the object is serialized with PDX, or to
   *        java fields on the object otherwise. The special field name
   *        {@link LuceneService#REGION_VALUE_FIELD} indicates that the entire value should be
   *        stored as a single field in the index.
   * @param analyzer The analyzer to use for this this field. Analyzers are used by Lucene to
   *        tokenize your field into individual words.
   */
  LuceneIndexFactory addField(String name, Analyzer analyzer);

  /**
   * Set the list of fields to be indexed.
   *
   * @param fieldMap Fields of the object to index, with the analyzer to be used for each field.
   *        Only fields listed here will be stored in the index. Fields should map to PDX fieldNames
   *        if the object is serialized with PDX, or to java fields on the object otherwise. The
   *        special field name {@link LuceneService#REGION_VALUE_FIELD} indicates that the entire
   *        value should be stored as a single field in the index.
   */
  LuceneIndexFactory setFields(Map<String, Analyzer> fieldMap);

  /**
   * Create the index on this member.
   *
   * @param indexName name of the index.
   * @param regionPath The region to index. The entries added to this region will be indexes.
   */
  void create(String indexName, String regionPath);

  /**
   * Configure the way objects are converted to lucene documents for this lucene index
   *
   * @param luceneSerializer A callback which converts a region value to a lucene document or
   *        documents to be stored in the index.
   *
   * @since Geode 1.4
   */
  LuceneIndexFactory setLuceneSerializer(LuceneSerializer luceneSerializer);
}
