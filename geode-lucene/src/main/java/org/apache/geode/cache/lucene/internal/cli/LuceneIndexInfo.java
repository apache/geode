/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.lucene.internal.cli;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;

import org.apache.lucene.analysis.Analyzer;

public class LuceneIndexInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String indexName;
  private final String regionPath;
  private final String[] searchableFieldNames;
  private final String[] fieldAnalyzers;

  public LuceneIndexInfo(final String indexName, final String regionPath, final String[] searchableFieldNames, String[] fieldAnalyzers) {
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.searchableFieldNames = searchableFieldNames;
    this.fieldAnalyzers = fieldAnalyzers;
  }

  public LuceneIndexInfo(final String indexName, final String regionPath) {
    this(indexName,regionPath,null,null);
  }

  public String getIndexName() {
    return indexName;
  }

  public String getRegionPath() {
    return regionPath;
  }

  public String[] getSearchableFieldNames() {
    return searchableFieldNames;
  }

  public String[] getFieldAnalyzers() {
    return fieldAnalyzers;
  }
}
