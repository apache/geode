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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
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
  private Map<String, VectorIndexMetadata> vectorFields = Collections.emptyMap();
  private final Map<String, Integer> indexStats;
  private final long segmentMemoryBytes;
  private final LuceneIndexStatus status;
  private String serializer;

  public LuceneIndexDetails(final String indexName, final String regionPath,
      final String[] searchableFieldNames, final Map<String, Analyzer> fieldAnalyzers,
      LuceneIndexStats indexStats, LuceneIndexStatus status, final String serverName,
      LuceneSerializer serializer) {
    this(indexName, regionPath, searchableFieldNames, fieldAnalyzers, Collections.emptyMap(),
        indexStats, status, serverName, serializer);
  }

  public LuceneIndexDetails(final String indexName, final String regionPath,
      final String[] searchableFieldNames, final Map<String, Analyzer> fieldAnalyzers,
      final Map<String, VectorIndexMetadata> vectorFields, LuceneIndexStats indexStats,
      LuceneIndexStatus status, final String serverName, LuceneSerializer serializer) {
    super(indexName, regionPath);
    this.serverName = serverName;
    this.searchableFieldNames =
        searchableFieldNames == null ? new String[0] : searchableFieldNames;
    this.fieldAnalyzers = getFieldAnalyzerStrings(fieldAnalyzers);
    this.vectorFields = vectorFields == null || vectorFields.isEmpty()
        ? Collections.emptyMap()
        : new LinkedHashMap<>(vectorFields);
    this.indexStats = getIndexStatsMap(indexStats);
    this.segmentMemoryBytes = indexStats == null ? 0L : indexStats.getSegmentMemoryBytes();
    this.status = status;
    this.serializer = serializer != null ? serializer.getClass().getSimpleName()
        : HeterogeneousLuceneSerializer.class.getSimpleName();
  }

  public LuceneIndexDetails(LuceneIndexImpl index, final String serverName) {
    this(index.getName(), index.getRegionPath(), index.getFieldNames(), index.getFieldAnalyzers(),
        index.getVectorFields(), index.getIndexStats(), LuceneIndexStatus.INITIALIZED, serverName,
        index.getLuceneSerializer());
  }

  public LuceneIndexDetails(LuceneIndexImpl index, final String serverName,
      final LuceneIndexStatus status) {
    this(index.getName(), index.getRegionPath(), index.getFieldNames(), index.getFieldAnalyzers(),
        index.getVectorFields(), index.getIndexStats(), status, serverName,
        index.getLuceneSerializer());
  }

  public LuceneIndexDetails(LuceneIndexCreationProfile indexProfile, final String serverName) {
    this(indexProfile.getIndexName(), indexProfile.getRegionPath(), indexProfile.getFieldNames(),
        null, indexProfile.getVectorFields(), null, LuceneIndexStatus.NOT_INITIALIZED, serverName,
        null);
    fieldAnalyzers = getFieldAnalyzerStringsFromProfile(indexProfile.getFieldAnalyzers());
    serializer = indexProfile.getSerializerClass();
  }

  public LuceneIndexDetails(LuceneIndexCreationProfile indexProfile, final String serverName,
      final LuceneIndexStatus status) {
    this(indexProfile.getIndexName(), indexProfile.getRegionPath(), indexProfile.getFieldNames(),
        null, indexProfile.getVectorFields(), null, status, serverName, null);
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

  public Map<String, VectorIndexMetadata> getVectorFields() {
    return vectorFields == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(vectorFields);
  }

  public String getVectorFieldsString() {
    if (vectorFields == null || vectorFields.isEmpty()) {
      return "{}";
    }
    Map<String, String> formatted = new LinkedHashMap<>();
    for (Entry<String, VectorIndexMetadata> entry : vectorFields.entrySet()) {
      VectorIndexMetadata meta = entry.getValue();
      formatted.put(entry.getKey(), String.format("(dim=%d, similarity=%s, M=%d, efC=%d, sq8=%s)",
          meta.getDimension(), meta.getSimilarityFunction(), meta.getMaxConnections(),
          meta.getBeamWidth(), meta.isQuantizeSQ8()));
    }
    return formatted.toString();
  }

  public long getSegmentMemoryBytes() {
    return segmentMemoryBytes;
  }

  @Override
  public String toString() {
    return "{\n\tIndex Name = " + indexName
        + ",\tRegion Path = " + regionPath
        + ",\tIndexed Fields = " + getSearchableFieldNamesString()
        + ",\tField Analyzer = " + getFieldAnalyzersString()
        + (vectorFields == null || vectorFields.isEmpty() ? ""
            : ",\tVector Fields = " + getVectorFieldsString())
        + ",\tSerializer = " + getSerializerString()
        + ",\tStatus =\n\t" + getStatus()
        + ",\tIndex Statistics =\n\t" + getIndexStatsString()
        + (vectorFields == null || vectorFields.isEmpty() ? ""
            : ",\tSegment Memory Bytes = " + segmentMemoryBytes)
        + "\n}\n";
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
