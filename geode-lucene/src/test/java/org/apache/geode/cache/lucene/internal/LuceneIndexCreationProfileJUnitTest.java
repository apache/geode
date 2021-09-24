/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static junitparams.JUnitParamsRunner.$;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_1;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_3;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_SERIALIZER;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junitparams.Parameters;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.lucene.DummyLuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class LuceneIndexCreationProfileJUnitTest {

  @Test
  @Parameters(method = "getSerializationProfiles")
  public void testSerialization(LuceneIndexCreationProfile profile) {
    LuceneServiceImpl.registerDataSerializables();
    LuceneIndexCreationProfile copy = CopyHelper.deepCopy(profile);
    assertEquals(profile.getIndexName(), copy.getIndexName());
    assertEquals(profile.getAnalyzerClass(), copy.getAnalyzerClass());
    assertArrayEquals(profile.getFieldNames(), copy.getFieldNames());
    assertEquals(profile.getFieldAnalyzers(), copy.getFieldAnalyzers());
  }

  private Object[] getSerializationProfiles() {
    return $(new Object[] {getOneFieldLuceneIndexCreationProfile()},
        new Object[] {getTwoFieldLuceneIndexCreationProfile()},
        new Object[] {getTwoAnalyzersLuceneIndexCreationProfile()},
        new Object[] {getDummySerializerCreationProfile()},
        new Object[] {getNullField1AnalyzerLuceneIndexCreationProfile()});
  }

  @Test
  @Parameters(method = "getProfileWithSerializer")
  public void toDataFromDataShouldContainSerializer(LuceneIndexCreationProfile profile,
      String expectedSerializerCLassName) throws IOException, ClassNotFoundException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(profile, hdos);
    byte[] outputArray = hdos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(outputArray);
    LuceneIndexCreationProfile profile2 = DataSerializer.readObject(new DataInputStream(bais));
    assertEquals(expectedSerializerCLassName, profile2.getSerializerClass());
  }

  private Object[] getProfileWithSerializer() {
    return $(new Object[] {getDefaultSerializerCreationProfile(), "HeterogeneousLuceneSerializer"},
        new Object[] {getDummySerializerCreationProfile(), "DummyLuceneSerializer"}, new Object[] {
            getHeterogeneousLuceneSerializerCreationProfile(), "HeterogeneousLuceneSerializer"});
  }

  private LuceneIndexCreationProfile getDefaultSerializerCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME, new String[] {"field1"},
        new StandardAnalyzer(), null, null);
  }

  private LuceneIndexCreationProfile getDummySerializerCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME, new String[] {"field1"},
        new StandardAnalyzer(), null, new DummyLuceneSerializer());
  }

  private LuceneIndexCreationProfile getHeterogeneousLuceneSerializerCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME, new String[] {"field1"},
        new StandardAnalyzer(), null, new HeterogeneousLuceneSerializer());
  }

  @Test
  @Parameters(method = "getCheckCompatibilityProfiles")
  public void testCheckCompatibility(LuceneIndexCreationProfile myProfile,
      LuceneIndexCreationProfile otherProfile, String expectedResult) {
    assertEquals(expectedResult,
        otherProfile.checkCompatibility(SEPARATOR + REGION_NAME, myProfile));
  }

  private Object[] getCheckCompatibilityProfiles() {
    return $(
        new Object[] {getOneFieldLuceneIndexCreationProfile(),
            getTwoFieldLuceneIndexCreationProfile(), CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS},
        new Object[] {getTwoFieldLuceneIndexCreationProfile(),
            getReverseFieldsLuceneIndexCreationProfile(), null},
        new Object[] {getTwoAnalyzersLuceneIndexCreationProfile(),
            getOneAnalyzerLuceneIndexCreationProfile(new KeywordAnalyzer()),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS},
        new Object[] {getOneAnalyzerLuceneIndexCreationProfile(new KeywordAnalyzer()),
            getTwoAnalyzersLuceneIndexCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_3},
        new Object[] {getOneAnalyzerLuceneIndexCreationProfile(new StandardAnalyzer()),
            getOneAnalyzerLuceneIndexCreationProfile(new KeywordAnalyzer()),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2},
        new Object[] {getNullField2AnalyzerLuceneIndexCreationProfile(),
            getNullField1AnalyzerLuceneIndexCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_1},
        new Object[] {getDefaultSerializerCreationProfile(), getDummySerializerCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_SERIALIZER},
        new Object[] {getDefaultSerializerCreationProfile(),
            getHeterogeneousLuceneSerializerCreationProfile(), null},
        new Object[] {getNullField1AnalyzerLuceneIndexCreationProfile(),
            getNullField2AnalyzerLuceneIndexCreationProfile(),
            LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2});
  }

  private LuceneIndexCreationProfile getOneFieldLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME, new String[] {"field1"},
        new StandardAnalyzer(), null, null);
  }

  private LuceneIndexCreationProfile getTwoFieldLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field1", "field2"}, new StandardAnalyzer(), null, null);
  }

  private LuceneIndexCreationProfile getReverseFieldsLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field2", "field1"}, new StandardAnalyzer(), null, null);
  }

  private LuceneIndexCreationProfile getOneAnalyzerLuceneIndexCreationProfile(Analyzer analyzer) {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", analyzer);
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field1", "field2"}, getPerFieldAnalyzerWrapper(fieldAnalyzers),
        fieldAnalyzers, null);
  }

  private LuceneIndexCreationProfile getTwoAnalyzersLuceneIndexCreationProfile() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new KeywordAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field1", "field2"}, getPerFieldAnalyzerWrapper(fieldAnalyzers),
        fieldAnalyzers, null);
  }

  private LuceneIndexCreationProfile getNullField1AnalyzerLuceneIndexCreationProfile() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", null);
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field1", "field2"}, getPerFieldAnalyzerWrapper(fieldAnalyzers),
        fieldAnalyzers, null);
  }

  private LuceneIndexCreationProfile getNullField2AnalyzerLuceneIndexCreationProfile() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new KeywordAnalyzer());
    fieldAnalyzers.put("field2", null);
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field1", "field2"}, getPerFieldAnalyzerWrapper(fieldAnalyzers),
        fieldAnalyzers, null);
  }

  private Analyzer getPerFieldAnalyzerWrapper(Map<String, Analyzer> fieldAnalyzers) {
    return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
  }
}
