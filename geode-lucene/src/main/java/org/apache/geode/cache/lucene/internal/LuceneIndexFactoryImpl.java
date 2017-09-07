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

import org.apache.geode.cache.lucene.LuceneIndexFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.util.LinkedHashMap;
import java.util.Map;

public class LuceneIndexFactoryImpl implements org.apache.geode.cache.lucene.LuceneIndexFactory {
  private final LuceneServiceImpl service;
  private final Map<String, Analyzer> fields = new LinkedHashMap<String, Analyzer>();


  public LuceneIndexFactoryImpl(final LuceneServiceImpl luceneService) {
    this.service = luceneService;
  }

  @Override
  public LuceneIndexFactory addField(final String name) {
    return addField(name, new StandardAnalyzer());
  }

  @Override
  public LuceneIndexFactory setFields(final String... fields) {
    this.fields.clear();
    for (String field : fields) {
      addField(field);
    }
    return this;
  }

  @Override
  public LuceneIndexFactory addField(final String name, final Analyzer analyzer) {
    fields.put(name, analyzer);
    return this;
  }

  @Override
  public LuceneIndexFactory setFields(final Map<String, Analyzer> fieldMap) {
    this.fields.clear();
    this.fields.putAll(fieldMap);
    return this;
  }

  @Override
  public void create(final String indexName, final String regionPath) {
    service.createIndex(indexName, regionPath, fields);

  }
}
