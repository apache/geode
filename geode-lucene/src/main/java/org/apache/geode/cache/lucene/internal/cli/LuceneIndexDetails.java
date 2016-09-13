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
package com.gemstone.gemfire.cache.lucene.internal.cli;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexCreationProfile;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexStats;

import org.apache.lucene.analysis.Analyzer;

public class LuceneIndexDetails implements Comparable<LuceneIndexDetails>, Serializable {
  private static final long serialVersionUID = 1L;
  private final String indexName;
  private final String regionPath;
  private final String[] searchableFieldNames;
  private Map<String, String> fieldAnalyzers=null;
  private final Map<String,Integer> indexStats;
  private boolean initialized;

  public LuceneIndexDetails(final String indexName, final String regionPath, final String[] searchableFieldNames, final Map<String, Analyzer> fieldAnalyzers, LuceneIndexStats indexStats, boolean initialized) {
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.searchableFieldNames = searchableFieldNames;
    this.fieldAnalyzers = getFieldAnalyzerStrings(fieldAnalyzers);
    this.indexStats=getIndexStatsMap(indexStats);
    this.initialized = initialized;
  }

  public LuceneIndexDetails(LuceneIndexImpl index) {
    this(index.getName(), index.getRegionPath(), index.getFieldNames(),index.getFieldAnalyzers(),index.getIndexStats(), true);
  }

  public LuceneIndexDetails(LuceneIndexCreationProfile indexProfile) {
    this(indexProfile.getIndexName(), indexProfile.getRegionPath(), indexProfile.getFieldNames(), null, null, false);
    this.fieldAnalyzers=getFieldAnalyzerStringsFromProfile(indexProfile.getFieldAnalyzers());
  }

  public Map<String,Integer> getIndexStats() {
    return indexStats;
  }
  private  Map<String,Integer> getIndexStatsMap(LuceneIndexStats indexStats) {
    Map<String,Integer> statsMap = new HashMap<>();
    if (indexStats==null) return statsMap;
    statsMap.put("queryExecutions",indexStats.getQueryExecutions());
    statsMap.put("updates",indexStats.getUpdates());
    statsMap.put("commits",indexStats.getCommits());
    statsMap.put("documents",indexStats.getDocuments());
    return statsMap;
  }

  public String getIndexStatsString() {
    return indexStats.toString();
  }

  private Map<String, String> getFieldAnalyzerStrings(Map<String, Analyzer> fieldAnalyzers) {
    if(fieldAnalyzers == null) {
      return Collections.emptyMap();
    }

    Map<String, String> results = new HashMap<>();

    for (Entry<String, Analyzer> entry : fieldAnalyzers.entrySet()) {
      final Analyzer analyzer = entry.getValue();
      if(analyzer != null) {
        results.put(entry.getKey(), analyzer.getClass().getSimpleName());
      }
    }
    return results;
  }

  private Map<String, String> getFieldAnalyzerStringsFromProfile(Map<String, String> fieldAnalyzers) {
    if(fieldAnalyzers == null) {
      return Collections.emptyMap();

    }

    Map<String, String> results = new HashMap<>();

    for (Entry<String, String> entry : fieldAnalyzers.entrySet()) {
      final String analyzer = entry.getValue();
      if(analyzer != null) {
        results.put(entry.getKey(), analyzer);
      }
    }
    return results;
  }

  public String getSearchableFieldNamesString() {
    return Arrays.asList(searchableFieldNames).toString();
  }


  public String getFieldAnalyzersString() {
    return fieldAnalyzers.toString();
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append("{\n\tIndex Name = "+indexName);
    buffer.append(",\tRegion Path = "+regionPath);
    buffer.append(",\tIndexed Fields = "+getSearchableFieldNamesString());
    buffer.append(",\tField Analyzer = "+getFieldAnalyzersString());
    buffer.append(",\tStatus =\n\t"+ getInitialized());
    buffer.append(",\tIndex Statistics =\n\t"+getIndexStatsString());
    buffer.append("\n}\n");
    return buffer.toString();
  }

  public boolean getInitialized() {
    return initialized;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getRegionPath() {
    return regionPath;
  }

  private static <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
    return (obj1 == null && obj2 == null ? 0 : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
  }

  @Override
  public int compareTo(final LuceneIndexDetails indexDetails) {
    int comparisonValue = compare(getIndexName(), indexDetails.getIndexName());
    return (comparisonValue != 0 ? comparisonValue : compare(getRegionPath(), indexDetails.getRegionPath()));
  }

}
