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

import org.apache.geode.cache.lucene.LuceneQueryFactory;

public class LuceneQueryInfo extends LuceneFunctionSerializable {
  private static final long serialVersionUID = 1L;
  private String queryString;
  private String defaultField;
  private int limit;
  private boolean keysOnly;

  public LuceneQueryInfo(final String indexName, final String regionPath, final String queryString,
      final String defaultField, final int limit, final boolean keysOnly) {
    super(indexName, regionPath);
    this.queryString = queryString;
    this.defaultField = defaultField;
    this.limit = limit;
    this.keysOnly = keysOnly;
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

}
