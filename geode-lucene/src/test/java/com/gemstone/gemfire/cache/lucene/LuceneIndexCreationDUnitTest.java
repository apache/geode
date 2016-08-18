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
package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.util.test.TestUtil;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.*;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCreationDUnitTest extends LuceneDUnitTest {

  @Override
  protected void initDataStore(SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }


  @Test
  @Parameters({"1", "2" , "10"})
  public void verifyThatIndexObjectsAreListedWhenPresentInTheSystem(int numberOfIndexes){
    SerializableRunnableIF createIndex = getMultipleIndexes(numberOfIndexes);
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore1.invoke(() -> verifyIndexList(numberOfIndexes));

    dataStore2.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> verifyIndexList(numberOfIndexes));
  }

  @Test
  @Parameters({"1", "2" , "10"})
  public void verifyThatIndexObjectIsRetrievedWhenPresentInTheSystem(int numberOfIndexes){
    SerializableRunnableIF createIndex = getMultipleIndexes(numberOfIndexes);
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore1.invoke(() -> verifyIndexes(numberOfIndexes));

    dataStore2.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> verifyIndexes(numberOfIndexes));
  }

  @Test
  public void verifyThatEmptyListIsOutputWhenThereAreNoIndexesInTheSystem(){
    dataStore1.invoke(() -> verifyIndexList(0));
    dataStore2.invoke(() -> verifyIndexList(0));
  }

  @Test
  public void verifyNullIsReturnedWhenGetIndexIsCalledAndNoIndexesArePresent(){
    dataStore1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      assertNull(luceneService.getIndex(INDEX_NAME,REGION_NAME));
    });

    dataStore2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      assertNull(luceneService.getIndex(INDEX_NAME,REGION_NAME));
    });
  }

  @Test
  public void verifyNullIsReturnedWhenGetIndexIsCalledWithNoMatchingIndex(){
    SerializableRunnableIF createIndex = get2FieldsIndexes();
    dataStore1.invoke(() -> createIndex);
    dataStore2.invoke(() -> createIndex);
    dataStore1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      assertNull(luceneService.getIndex(INDEX_NAME+"_A",REGION_NAME));
    });

    dataStore2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      assertNull(luceneService.getIndex(INDEX_NAME+"_A",REGION_NAME));
    });
  }


  @Test
  public void verifyDifferentFieldsFails() {
    SerializableRunnableIF createIndex1 = getFieldsIndexWithOneField();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getFieldsIndexWithTwoFields();
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS));
  }

  @Test
  public void verifyDifferentFieldAnalyzerSizesFails1() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithTwoFields();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithOneField();
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS_2));
  }

  @Test
  public void verifyDifferentFieldAnalyzerSizesFails2() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithOneField();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithTwoFields();
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS));
  }

  @Test
  public void verifyDifferentFieldAnalyzersFails1() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithOneField(StandardAnalyzer.class);
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithOneField(KeywordAnalyzer.class);
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2));
  }

  @Test
  public void verifyDifferentFieldAnalyzersFails2() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithNullField1();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithNullField2();
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS));
  }

  @Test
  public void verifyDifferentFieldAnalyzersFails3() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithNullField2();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithNullField1();
    dataStore2.invoke(() -> initDataStore(createIndex2,
      LuceneTestUtilities.CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_3));
  }

  @Test
  public void verifyDifferentIndexNamesFails() {
    SerializableRunnableIF createIndex1 = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME+"1", REGION_NAME, "field1");
    };
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME+"2", REGION_NAME, "field1");
    };
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_NAMES));
  }

  @Test
  public void verifyDifferentIndexesFails1() {
    SerializableRunnableIF createIndex1 = getFieldsIndexWithOneField();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = () -> {/*Do nothing*/};
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_INDEXES_1));
  }

  @Test
  public void verifyDifferentIndexesFails2() {
    SerializableRunnableIF createIndex1 = getFieldsIndexWithOneField();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "field1");
      luceneService.createIndex(INDEX_NAME+"2", REGION_NAME, "field2");
    };
    dataStore2.invoke(() -> initDataStore(createIndex2, CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_INDEXES_2));
  }

  @Test
  @Parameters(method = "getIndexes")
  public void verifySameIndexesSucceeds(SerializableRunnableIF createIndex) {
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> initDataStore(createIndex));
  }

  private final Object[] getIndexes() {
    return $(
        new Object[] { getFieldsIndexWithOneField() },
        new Object[] { getFieldsIndexWithTwoFields() },
        new Object[] { get2FieldsIndexes() },
        new Object[] { getAnalyzersIndexWithOneField() },
        new Object[] { getAnalyzersIndexWithTwoFields() },
        new Object[] { getAnalyzersIndexWithNullField1() }
    );
  }

  @Test
  @Parameters(method = "getXmlAndExceptionMessages")
  public void verifyXml(String cacheXmlFileBaseName, String exceptionMessage) {
    dataStore1.invoke(() -> initCache(getXmlFileForTest(cacheXmlFileBaseName + ".1")));
    dataStore2.invoke(() -> initCache(getXmlFileForTest(cacheXmlFileBaseName + ".2"), exceptionMessage));
  }

  @Test
  public void verifyXMLMultipleIndexList() {
    dataStore1.invoke(() -> initCache(getXmlFileForTest("verifyXMLMultipleIndexList")));
    dataStore2.invoke(() -> initCache(getXmlFileForTest("verifyXMLMultipleIndexList")));

    dataStore1.invoke(() -> verifyIndexList(2));
    dataStore2.invoke(() -> verifyIndexList(2));
  }

  @Test
  public void verifyXMLMultipleIndexes() {
    dataStore1.invoke(() -> initCache(getXmlFileForTest("verifyXMLMultipleIndexList")));
    dataStore2.invoke(() -> initCache(getXmlFileForTest("verifyXMLMultipleIndexList")));

    dataStore1.invoke(() -> verifyIndexes(2));
    dataStore2.invoke(() -> verifyIndexes(2));
  }

  @Test
  public void verifyXMLEmptyIndexList() {
    dataStore1.invoke(() -> initCache(getXmlFileForTest("verifyXMLEmptyIndexList")));
    dataStore2.invoke(() -> initCache(getXmlFileForTest("verifyXMLEmptyIndexList")));

    dataStore1.invoke(() -> verifyIndexList(0));
    dataStore2.invoke(() -> verifyIndexList(0));
  }



  private final Object[] getXmlAndExceptionMessages() {
    return $(
        new Object[] { "verifyDifferentFieldsFails", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS },
        new Object[] { "verifyDifferentFieldAnalyzerSizesFails1", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS_2 },
        new Object[] { "verifyDifferentFieldAnalyzerSizesFails2", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_FIELDS },
        new Object[] { "verifyDifferentFieldAnalyzersFails1", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2 },
        // Currently setting a null analyzer is not a valid xml configuration: <lucene:field name="field2" analyzer="null"/>
        //new Object[] { "verifyDifferentFieldAnalyzersFails2", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_1 },
        //new Object[] { "verifyDifferentFieldAnalyzersFails3", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_ANALYZERS_2 },
        new Object[] { "verifyDifferentIndexNamesFails", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_NAMES },
        new Object[] { "verifyDifferentIndexesFails1", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_INDEXES_1 },
        new Object[] { "verifyDifferentIndexesFails2", CANNOT_CREATE_LUCENE_INDEX_DIFFERENT_INDEXES_2 }
    );
  }

  @Test
  public void verifyStandardAnalyzerAndNullOnSameFieldPasses() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithNullField1();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithTwoFields2();
    dataStore2.invoke(() -> initDataStore(createIndex2));
  }

  @Test
  public void verifyStandardAnalyzerAndNullOnSameFieldPasses2() {
    SerializableRunnableIF createIndex1 = getAnalyzersIndexWithTwoFields2();
    dataStore1.invoke(() -> initDataStore(createIndex1));

    SerializableRunnableIF createIndex2 = getAnalyzersIndexWithNullField1();
    dataStore2.invoke(() -> initDataStore(createIndex2));
  }

  private String getXmlFileForTest(String testName) {
    return TestUtil.getResourcePath(getClass(), getClass().getSimpleName() + "." + testName + ".cache.xml");
  }

  private void initDataStore(SerializableRunnableIF createIndex, String message) throws Exception {
    createIndex.run();
    try {
      getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
      fail("Should not have been able to create index");
    } catch (IllegalStateException e) {
      assertEquals(message, e.getMessage());
    }
  }

  private void initCache(String cacheXmlFileName) throws FileNotFoundException {
    getCache().loadCacheXml(new FileInputStream(cacheXmlFileName));
  }

  private void initCache(String cacheXmlFileName, String message) throws FileNotFoundException {
    try {
      getCache().loadCacheXml(new FileInputStream(cacheXmlFileName));
      fail("Should not have been able to create cache");
    } catch (IllegalStateException e) {
      assertEquals(message, e.getMessage());
    }
  }

  private SerializableRunnableIF getFieldsIndexWithOneField() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "field1");
    };
  }

  private SerializableRunnableIF getFieldsIndexWithTwoFields() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "field1", "field2");
    };
  }

  private SerializableRunnableIF get2FieldsIndexes() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME+"_1", REGION_NAME, "field1");
      luceneService.createIndex(INDEX_NAME+"_2", REGION_NAME, "field2");
    };
  }

  private SerializableRunnableIF getMultipleIndexes(final int numberOfIndexes) {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      for(int count = 1 ; count <= numberOfIndexes; count++){
        luceneService.createIndex(INDEX_NAME+"_"+count, REGION_NAME, "field"+count);
      }
    };
  }

  private void verifyIndexList(final int expectedSize) {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    Collection<LuceneIndex> indexList = luceneService.getAllIndexes();
    assertEquals(indexList.size() , expectedSize);
  }

  private void verifyIndexes(final int numberOfIndexes) {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    for(int count = 1; count <= numberOfIndexes; count++){
      assertEquals(luceneService.getIndex(INDEX_NAME+"_"+count, REGION_NAME).getName(), INDEX_NAME+"_"+count);
    }
  }

  private SerializableRunnableIF getAnalyzersIndexWithNullField1() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> analyzers = new HashMap<>();
      analyzers.put("field1", null);
      analyzers.put("field2", new KeywordAnalyzer());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    };
  }

  private SerializableRunnableIF getAnalyzersIndexWithNullField2() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> analyzers = new HashMap<>();
      analyzers.put("field1", new KeywordAnalyzer());
      analyzers.put("field2", null);
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    };
  }

  private SerializableRunnableIF getAnalyzersIndexWithOneField(Class<? extends Analyzer> analyzerClass) {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> analyzers = new HashMap<>();
      analyzers.put("field1", analyzerClass.newInstance());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    };
  }

  private SerializableRunnableIF getAnalyzersIndexWithOneField() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> analyzers = new HashMap<>();
      analyzers.put("field1", new KeywordAnalyzer());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    };
  }

  private SerializableRunnableIF getAnalyzersIndexWithTwoFields() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> analyzers = new HashMap<>();
      analyzers.put("field1", new KeywordAnalyzer());
      analyzers.put("field2", new KeywordAnalyzer());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    };
  }

  private SerializableRunnableIF getAnalyzersIndexWithTwoFields2() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> analyzers = new HashMap<>();
      analyzers.put("field1", new StandardAnalyzer());
      analyzers.put("field2", new KeywordAnalyzer());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    };
  }
}
