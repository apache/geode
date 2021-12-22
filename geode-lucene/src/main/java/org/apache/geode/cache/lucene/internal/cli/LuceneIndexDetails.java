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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;

public class LuceneIndexDetails extends LuceneFunctionSerializable
    implements Comparable<LuceneIndexDetails> {
  private static final long serialVersionUID = 1L;
  private final String serverName;
  private final String[] searchableFieldNames;
  private Map<String, String> fieldAnalyzers = null;
  private final Map<String, Integer> indexStats;
  private final LuceneIndexStatus status;
  private String serializer;

  public LuceneIndexDetails(final String indexName, final String regionPath,
      final String[] searchableFieldNames, final Map<String, Analyzer> fieldAnalyzers,
      LuceneIndexStats indexStats, LuceneIndexStatus status, final String serverName,
      LuceneSerializer serializer) {
    super(indexName, regionPath);
    this.serverName = serverName;
    this.searchableFieldNames = searchableFieldNames;
    this.fieldAnalyzers = getFieldAnalyzerStrings(fieldAnalyzers);
    this.indexStats = getIndexStatsMap(indexStats);
    this.status = status;
    this.serializer = serializer != null ? serializer.getClass().getSimpleName()
        : HeterogeneousLuceneSerializer.class.getSimpleName();
  }

  public LuceneIndexDetails(LuceneIndexImpl index, final String serverName) {
    this(index.getName(), index.getRegionPath(), index.getFieldNames(), index.getFieldAnalyzers(),
        index.getIndexStats(), LuceneIndexStatus.INITIALIZED, serverName,
        index.getLuceneSerializer());
  }

  public LuceneIndexDetails(LuceneIndexImpl index, final String serverName,
      final LuceneIndexStatus status) {
    this(index.getName(), index.getRegionPath(), index.getFieldNames(), index.getFieldAnalyzers(),
        index.getIndexStats(), status, serverName, index.getLuceneSerializer());
  }

  public LuceneIndexDetails(LuceneIndexCreationProfile indexProfile, final String serverName) {
    this(indexProfile.getIndexName(), indexProfile.getRegionPath(), indexProfile.getFieldNames(),
        null, null, LuceneIndexStatus.NOT_INITIALIZED, serverName, null);
    fieldAnalyzers = getFieldAnalyzerStringsFromProfile(indexProfile.getFieldAnalyzers());
    serializer = indexProfile.getSerializerClass();
  }

  public LuceneIndexDetails(LuceneIndexCreationProfile indexProfile, final String serverName,
      final LuceneIndexStatus status) {
    this(indexProfile.getIndexName(), indexProfile.getRegionPath(), indexProfile.getFieldNames(),
        null, null, status, serverName, null);
    fieldAnalyzers = getFieldAnalyzerStringsFromProfile(indexProfile.getFieldAnalyzers());
    serializer = indexProfile.getSerializerClass();
  }

  public Map<String, Integer> getIndexStats() {
    return indexStats;
  }

  private Map<String, Integer> getIndexStatsMap(LuceneIndexStats indexStats) {
    Map<String, Integer> statsMap = new HashMap<>();
    if (indexStats == null) {
      return statsMap;
    }
    statsMap.put("queryExecutions", indexStats.getQueryExecutions());
    statsMap.put("updates", indexStats.getUpdates());
    statsMap.put("commits", indexStats.getCommits());
    statsMap.put("documents", indexStats.getDocuments());
    return statsMap;
  }

  public String getIndexStatsString() {
    return indexStats.toString();
  }

  private Map<String, String> getFieldAnalyzerStrings(Map<String, Analyzer> fieldAnalyzers) {
    if (fieldAnalyzers == null) {
      return Collections.emptyMap();
    }

    Map<String, String> results = new HashMap<>();

    for (Entry<String, Analyzer> entry : fieldAnalyzers.entrySet()) {
      final Analyzer analyzer = entry.getValue();
      if (analyzer != null) {
        results.put(entry.getKey(), analyzer.getClass().getSimpleName());
      }
    }
    return results;
  }

  private Map<String, String> getFieldAnalyzerStringsFromProfile(
      Map<String, String> fieldAnalyzers) {
    if (fieldAnalyzers == null) {
      return Collections.emptyMap();

    }

    Map<String, String> results = new HashMap<>();

    for (Entry<String, String> entry : fieldAnalyzers.entrySet()) {
      final String analyzer = entry.getValue();
      if (analyzer != null) {
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

  public String getSerializerString() {
    return serializer;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("{\n\tIndex Name = " + indexName);
    buffer.append(",\tRegion Path = " + regionPath);
    buffer.append(",\tIndexed Fields = " + getSearchableFieldNamesString());
    buffer.append(",\tField Analyzer = " + getFieldAnalyzersString());
    buffer.append(",\tSerializer = " + getSerializerString());
    buffer.append(",\tStatus =\n\t" + getStatus());
    buffer.append(",\tIndex Statistics =\n\t" + getIndexStatsString());
    buffer.append("\n}\n");
    return buffer.toString();
  }

  public LuceneIndexStatus getStatus() {
    return status;
  }

  private static <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
    return (obj1 == null && obj2 == null ? 0
        : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
  }

  @Override
  public int compareTo(final LuceneIndexDetails indexDetails) {
    int comparisonValue = compare(getIndexName(), indexDetails.getIndexName());
    return (comparisonValue != 0 ? comparisonValue
        : compare(getRegionPath(), indexDetails.getRegionPath()));
  }

  public String getServerName() {
    return serverName;
  }
}
