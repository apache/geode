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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.internal.VersionedDataSerializable;
import org.apache.geode.internal.cache.CacheServiceProfile;
import org.apache.geode.internal.serialization.KnownVersion;

public class LuceneIndexCreationProfile implements CacheServiceProfile, VersionedDataSerializable {

  private String indexName;

  private String[] fieldNames;

  private String analyzerClass;

  private Map<String, String> fieldAnalyzers;

  private String serializerClass = HeterogeneousLuceneSerializer.class.getSimpleName();

  private String regionPath;

  /* Used by DataSerializer */
  public LuceneIndexCreationProfile() {}

  public LuceneIndexCreationProfile(String indexName, String regionPath, String[] fieldNames,
      Analyzer analyzer, Map<String, Analyzer> fieldAnalyzers, LuceneSerializer serializer) {
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.fieldNames = fieldNames;
    analyzerClass = analyzer.getClass().getSimpleName();
    initializeFieldAnalyzers(fieldAnalyzers);
    if (serializer != null) {
      serializerClass = serializer.getClass().getSimpleName();
    }
  }

  public String getIndexName() {
    return indexName;
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  public String getAnalyzerClass() {
    return analyzerClass;
  }

  public Map<String, String> getFieldAnalyzers() {
    return fieldAnalyzers;
  }

  public String getSerializerClass() {
    return serializerClass;
  }

  protected void initializeFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    this.fieldAnalyzers = new HashMap<>();
    for (String field : fieldNames) {
      if (fieldAnalyzers != null && !fieldAnalyzers.isEmpty()) {
        this.fieldAnalyzers.put(field,
            fieldAnalyzers.get(field) == null ? StandardAnalyzer.class.getSimpleName()
                : fieldAnalyzers.get(field).getClass().getSimpleName());
      } else {
        this.fieldAnalyzers.put(field, StandardAnalyzer.class.getSimpleName());
      }
    }
  }

  @Override
  public String getId() {
    return generateId(indexName, regionPath);
  }

  public static String generateId(String indexName, String regionPath) {
    return "lucene_" + LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
  }

  @Override
  public String checkCompatibility(String regionPath, CacheServiceProfile profile) {
    String result = null;
    LuceneIndexCreationProfile remoteProfile = (LuceneIndexCreationProfile) profile;

    // Verify fields are the same
    if ((getFieldNames().length != remoteProfile.getFieldNames().length) || (!Arrays
        .asList(getFieldNames()).containsAll(Arrays.asList(remoteProfile.getFieldNames())))) {
      return String.format(
          "Cannot create Lucene index %s on region %s with fields %s because another member defines the same index with fields %s.",
          getIndexName(), regionPath, Arrays.toString(getFieldNames()),
          Arrays.toString(remoteProfile.getFieldNames()));
    }

    // Verify the analyzer class is the same
    // Note: This test will currently only fail if per-field analyzers are used in one member but
    // not another,
    // This condition will be caught in the tests below so this test is commented out. If we ever
    // allow the user
    // to configure a single analyzer for all fields, then this test will be useful again.
    /*
     * if (!remoteLuceneIndexProfile.getAnalyzerClass().isInstance(getAnalyzer())) { result =
     * LocalizedStrings.
     * String.
     * format("Cannot create Lucene index %s on region %s with analyzer %s because another member defines the same index with analyzer %s."
     * ,
     * .toString(indexName, regionPath, remoteLuceneIndexProfile.getAnalyzerClass().getName(),
     * analyzer.getClass().getName()); }
     */

    // Iterate the existing analyzers and compare them to the input analyzers
    // Note: This is currently destructive to the input field analyzers map which should be ok
    // since its a transient object.
    if (!getFieldAnalyzers().equals(remoteProfile.getFieldAnalyzers())) {
      if (getFieldAnalyzers().size() != remoteProfile.getFieldAnalyzers().size()) {
        return String.format(
            "Cannot create Lucene index %s on region %s with field analyzers %s because another member defines the same index with field analyzers %s.",
            getIndexName(), regionPath,
            Arrays.toString(getFieldAnalyzers().values().toArray()),
            Arrays.toString(remoteProfile.getFieldAnalyzers().values().toArray()));
      }
      // now the 2 maps should have the same size
      for (String field : getFieldAnalyzers().keySet()) {
        if (!remoteProfile.getFieldAnalyzers().get(field).equals(getFieldAnalyzers().get(field))) {
          return String.format(
              "Cannot create Lucene index %s on region %s with analyzer %s on field %s because another member defines the same index with analyzer %s on that field.",
              getIndexName(), regionPath, getFieldAnalyzers().get(field), field,
              remoteProfile.getFieldAnalyzers().get(field));
        }
      }
    }

    if (!getSerializerClass().equals(remoteProfile.getSerializerClass())) {
      return String.format(
          "Cannot create Lucene index %s on region %s with serializer %s because another member defines the same index with different serializer %s.",
          getIndexName(), regionPath, getSerializerClass(),
          remoteProfile.getSerializerClass());
    }

    return result;
  }

  @Override
  public String getMissingProfileMessage(boolean existsInThisMember) {
    return existsInThisMember
        ? String.format(
            "Cannot create Lucene index %s on region %s because it is not defined in another member.",
            getIndexName(), regionPath)
        : String.format(
            "Must create Lucene index %s on region %s because it is defined in another member.",
            getIndexName(), regionPath);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    toDataPre_GEODE_1_4_0_0(out);
    DataSerializer.writeString(serializerClass, out);
  }

  public void toDataPre_GEODE_1_4_0_0(DataOutput out) throws IOException {
    DataSerializer.writeString(indexName, out);
    DataSerializer.writeString(regionPath, out);
    DataSerializer.writeStringArray(fieldNames, out);
    DataSerializer.writeString(analyzerClass, out);
    DataSerializer.writeHashMap(fieldAnalyzers, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    fromDataPre_GEODE_1_4_0_0(in);
    serializerClass = DataSerializer.readString(in);
  }

  public void fromDataPre_GEODE_1_4_0_0(DataInput in) throws IOException, ClassNotFoundException {
    indexName = DataSerializer.readString(in);
    regionPath = DataSerializer.readString(in);
    fieldNames = DataSerializer.readStringArray(in);
    analyzerClass = DataSerializer.readString(in);
    fieldAnalyzers = DataSerializer.readHashMap(in);
  }

  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[").append("indexName=")
        .append(indexName).append("; regionPath=").append(regionPath)
        .append("; fieldNames=").append(Arrays.toString(fieldNames)).append("; analyzerClass=")
        .append(analyzerClass).append("; fieldAnalyzers=").append(fieldAnalyzers)
        .append("; serializer=").append(serializerClass).append("]").toString();
  }

  public String getRegionPath() {
    return regionPath;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return new KnownVersion[] {KnownVersion.GEODE_1_4_0};
  }
}
