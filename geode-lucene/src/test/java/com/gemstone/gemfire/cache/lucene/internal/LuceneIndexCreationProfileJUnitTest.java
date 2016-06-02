/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
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

  private final Object[] getSerializationProfiles() {
    return $(
        new Object[] { getOneFieldLuceneIndexCreationProfile() },
        new Object[] { getTwoFieldLuceneIndexCreationProfile() },
        new Object[] { getTwoAnalyzersLuceneIndexCreationProfile() },
        new Object[] { getNullField1AnalyzerLuceneIndexCreationProfile() }
    );
  }

  @Test
  @Parameters(method = "getCheckCompatibilityProfiles")
  public void testCheckCompatibility(LuceneIndexCreationProfile myProfile, LuceneIndexCreationProfile otherProfile, String expectedResult) {
    assertEquals(expectedResult, otherProfile.checkCompatibility("/"+REGION_NAME, myProfile));
  }

  private final Object[] getCheckCompatibilityProfiles() {
    return $(
        new Object[] {
            getOneFieldLuceneIndexCreationProfile(),
            getTwoFieldLuceneIndexCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS
        },
        new Object[] {
            getTwoAnalyzersLuceneIndexCreationProfile(),
            getOneAnalyzerLuceneIndexCreationProfile(new KeywordAnalyzer()),
            CANNOT_CREATE_LUCENE_INDEX_NO_ANALYZER_FIELD2
        },
        new Object[] {
            getOneAnalyzerLuceneIndexCreationProfile(new KeywordAnalyzer()),
            getTwoAnalyzersLuceneIndexCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZER_SIZES_2
        },
        new Object[] {
            getOneAnalyzerLuceneIndexCreationProfile(new StandardAnalyzer()),
            getOneAnalyzerLuceneIndexCreationProfile(new KeywordAnalyzer()),
            CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS
        },
        new Object[] {
            getNullField2AnalyzerLuceneIndexCreationProfile(),
            getNullField1AnalyzerLuceneIndexCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_NO_ANALYZER_FIELD1
        },
        new Object[] {
            getNullField1AnalyzerLuceneIndexCreationProfile(),
            getNullField2AnalyzerLuceneIndexCreationProfile(),
            CANNOT_CREATE_LUCENE_INDEX_NO_ANALYZER_EXISTING_MEMBER
        }
    );
  }

  private LuceneIndexCreationProfile getOneFieldLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, new String[] { "field1" }, new StandardAnalyzer(), null);
  }

  private LuceneIndexCreationProfile getTwoFieldLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, new String[] { "field1", "field2" }, new StandardAnalyzer(), null);
  }

  private LuceneIndexCreationProfile getOneAnalyzerLuceneIndexCreationProfile(Analyzer analyzer) {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", analyzer);
    return new LuceneIndexCreationProfile(INDEX_NAME, new String[] { "field1", "field2" }, getPerFieldAnalyzerWrapper(fieldAnalyzers), fieldAnalyzers);
  }

  private LuceneIndexCreationProfile getTwoAnalyzersLuceneIndexCreationProfile() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new KeywordAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    return new LuceneIndexCreationProfile(INDEX_NAME, new String[] { "field1", "field2" }, getPerFieldAnalyzerWrapper(fieldAnalyzers), fieldAnalyzers);
  }

  private LuceneIndexCreationProfile getNullField1AnalyzerLuceneIndexCreationProfile() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", null);
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    return new LuceneIndexCreationProfile(INDEX_NAME, new String[] { "field1", "field2" }, getPerFieldAnalyzerWrapper(fieldAnalyzers), fieldAnalyzers);
  }

  private LuceneIndexCreationProfile getNullField2AnalyzerLuceneIndexCreationProfile() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new KeywordAnalyzer());
    fieldAnalyzers.put("field2", null);
    return new LuceneIndexCreationProfile(INDEX_NAME, new String[] { "field1", "field2" }, getPerFieldAnalyzerWrapper(fieldAnalyzers), fieldAnalyzers);
  }

  private Analyzer getPerFieldAnalyzerWrapper(Map<String, Analyzer> fieldAnalyzers) {
    return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
  }
}
