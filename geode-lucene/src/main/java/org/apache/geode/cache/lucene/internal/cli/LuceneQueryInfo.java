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

package org.apache.geode.cache.lucene.internal.cli;

import java.util.Arrays;

import org.apache.geode.cache.lucene.LuceneQueryFactory;

public class LuceneQueryInfo extends LuceneFunctionSerializable {
  private static final long serialVersionUID = 1L;
  private final String queryString;
  private final String defaultField;
  private final int limit;
  private final boolean keysOnly;
  private final String vectorField;
  private final float[] queryVector;
  private final int k;
  private final String filterQuery;

  public LuceneQueryInfo(final String indexName, final String regionPath, final String queryString,
      final String defaultField, final int limit, final boolean keysOnly) {
    this(indexName, regionPath, queryString, defaultField, limit, keysOnly, null, null, -1, null);
  }

  public LuceneQueryInfo(final String indexName, final String regionPath, final String queryString,
      final String defaultField, final int limit, final boolean keysOnly,
      final String vectorField, final float[] queryVector, final int k, final String filterQuery) {
    super(indexName, regionPath);
    this.queryString = queryString;
    this.defaultField = defaultField;
    this.limit = limit;
    this.keysOnly = keysOnly;
    this.vectorField = vectorField;
    this.queryVector = queryVector == null ? null : Arrays.copyOf(queryVector, queryVector.length);
    this.k = k;
    this.filterQuery = filterQuery;
  }

  public String getQueryString() {
    return queryString;
  }

  public String getDefaultField() {
    return defaultField;
  }

  public int getLimit() {
    if (limit == -1) {
      return LuceneQueryFactory.DEFAULT_LIMIT;
    } else {
      return limit;
    }
  }

  public boolean getKeysOnly() {
    return keysOnly;
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

  public int getEffectiveK() {
    return k > 0 ? k : getLimit();
  }

  public String getFilterQuery() {
    return filterQuery;
  }

  public boolean isVectorQuery() {
    return queryVector != null && queryVector.length > 0;
  }

}
