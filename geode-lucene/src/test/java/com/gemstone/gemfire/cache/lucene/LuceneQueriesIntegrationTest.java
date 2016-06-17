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
package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.test.TestObject;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This class contains tests of lucene queries that can fit
 */
@Category(IntegrationTest.class)
public class LuceneQueriesIntegrationTest extends LuceneIntegrationTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private static final String INDEX_NAME = "index";
  protected static final String REGION_NAME = "index";

  @Test()
  public void shouldNotTokenizeWordsWithKeywordAnalyzer() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("field1", new StandardAnalyzer());
    fields.put("field2", new KeywordAnalyzer());
    luceneService.createIndex(INDEX_NAME, REGION_NAME, fields);
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    //Put two values with some of the same tokens
    String value1 = "one three";
    String value2 = "one two three";
    String value3 = "one@three";
    region.put("A", new TestObject(value1, value1));
    region.put("B", new TestObject(value2, value2));
    region.put("C", new TestObject(value3, value3));

    // The value will be tokenized into following documents using the analyzers:
    // <field1:one three> <field2:one three>
    // <field1:one two three> <field2:one two three>
    // <field1:one@three> <field2:one@three>
    
    index.waitUntilFlushed(60000);

    // standard analyzer with double quote
    // this query string will be parsed as "one three"
    // but standard analyzer will parse value "one@three" to be "one three"
    // query will be--fields1:"one three"
    // so C will be hit by query
    verifyQuery("field1:\"one three\"", DEFAULT_FIELD, "A", "C");
    
    // standard analyzer will not tokenize by '_'
    // this query string will be parsed as "one_three"
    // query will be--field1:one_three
    verifyQuery("field1:one_three", DEFAULT_FIELD);
    
    // standard analyzer will tokenize by '@'
    // this query string will be parsed as "one" "three"
    // query will be--field1:one field1:three
    verifyQuery("field1:one@three", DEFAULT_FIELD, "A", "B", "C");
    
    HashMap expectedResults = new HashMap();
    expectedResults.put("A", new TestObject(value1, value1));
    expectedResults.put("B", new TestObject(value2, value2));
    expectedResults.put("C", new TestObject(value3, value3));
    verifyQuery("field1:one@three", DEFAULT_FIELD, expectedResults);
    
    // keyword analyzer, this query will only match the entry that exactly matches
    // this query string will be parsed as "one three"
    // but keyword analyzer will parse one@three to be "one three"
    // query will be--field2:one three
    verifyQuery("field2:\"one three\"", DEFAULT_FIELD, "A");

    // keyword analyzer without double quote. It should be the same as 
    // with double quote
    // query will be--field2:one@three
    verifyQuery("field2:one@three", DEFAULT_FIELD, "C");
  }

  @Test()
  public void shouldPaginateResults() throws Exception {

    luceneService.createIndex(INDEX_NAME, REGION_NAME, "field1", "field2");
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    //Put two values with some of the same tokens
    String value1 = "one three";
    String value2 = "one two three";
    String value3 = "one@three";
    region.put("A", new TestObject(value1, value1));
    region.put("B", new TestObject(value2, value2));
    region.put("C", new TestObject(value3, value3));

    index.waitUntilFlushed(60000);
    final LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory()
      .setPageSize(2)
      .create(INDEX_NAME, REGION_NAME,
      "one", "field1");

    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    assertTrue(pages.hasNextPage());
    assertEquals(3, pages.size());
    final List<LuceneResultStruct<Object, Object>> page1 = pages.getNextPage();
    final List<LuceneResultStruct<Object, Object>> page2 = pages.getNextPage();
    List<LuceneResultStruct<Object, Object>> allEntries=new ArrayList<>();
    allEntries.addAll(page1);
    allEntries.addAll(page2);

    assertEquals(region.keySet(), allEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet()));
    assertEquals(region.values(), allEntries.stream().map(entry -> entry.getValue()).collect(Collectors.toSet()));

  }

  @Test()
  public void shouldTokenizeUsingMyCharacterAnalyser() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    // not to specify field1's analyzer, it should use standard analyzer
    // Note: fields has to contain "field1", otherwise, field1 will not be tokenized
    fields.put("field1", null);
    fields.put("field2", new MyCharacterAnalyzer());
    luceneService.createIndex(INDEX_NAME, REGION_NAME, fields);
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    //Put two values with some of the same tokens
    String value1 = "one three";
    String value4 = "two_four";
    String value3 = "two@four";
    region.put("A", new TestObject(value1, value4));
    region.put("B", new TestObject(value1, value3));
    region.put("C", new TestObject(value3, value3));
    region.put("D", new TestObject(value4, value4));

    index.waitUntilFlushed(60000);

    verifyQuery("field1:one AND field2:two_four", DEFAULT_FIELD, "A");
    verifyQuery("field1:one AND field2:two", DEFAULT_FIELD, "A");
    verifyQuery("field1:three AND field2:four", DEFAULT_FIELD, "A");
  }

  @Test()
  public void shouldAllowNullInFieldValue() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("field1", null);
    fields.put("field2", null);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, fields);
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    //Put two values with some of the same tokens
    String value1 = "one three";
    region.put("A", new TestObject(value1, null));
    index.waitUntilFlushed(60000);

    verifyQuery("field1:one", DEFAULT_FIELD, "A");
  }

  @Test()
  public void queryJsonObject() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("name", null);
    fields.put("lastName", null);
    fields.put("address", null);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, fields);
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    //Put two values with some of the same tokens
    PdxInstance pdx1 = insertAJson(region, "jsondoc1");
    PdxInstance pdx2 = insertAJson(region, "jsondoc2");
    PdxInstance pdx10 = insertAJson(region, "jsondoc10");
    index.waitUntilFlushed(60000);

    HashMap expectedResults = new HashMap();
    expectedResults.put("jsondoc1", pdx1);
    expectedResults.put("jsondoc10", pdx10);
    verifyQuery("name:jsondoc1*", DEFAULT_FIELD, expectedResults);
  }

  @Test()
  public void shouldAllowQueryOnRegionWithStringValue() throws Exception {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, LuceneService.REGION_VALUE_FIELD);
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    region.put("A", "one three");
    index.waitUntilFlushed(60000);

    verifyQuery("one", LuceneService.REGION_VALUE_FIELD, "A");
  }

  @Test()
  public void throwFunctionExceptionWhenGivenBadQuery() throws Exception {
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);

    //Create a query that throws an exception
    final LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
      (index) -> {
        throw new LuceneQueryException("Bad query");
      });


    thrown.expect(LuceneQueryException.class);
    query.findPages();
  }
  
  private PdxInstance insertAJson(Region region, String key) {
    String jsonCustomer = "{"
        + "\"name\": \""+key+"\","
        + "\"lastName\": \"Smith\","
        + " \"age\": 25,"
        + "\"address\":"
        + "{"
        + "\"streetAddress\": \"21 2nd Street\","
        + "\"city\": \"New York\","
        + "\"state\": \"NY\","
        + "\"postalCode\": \"10021\""
        + "},"
        + "\"phoneNumber\":"
        + "["
        + "{"
        + " \"type\": \"home\","
        + "\"number\": \"212 555-1234\""
        + "},"
        + "{"
        + " \"type\": \"fax\","
        + "\"number\": \"646 555-4567\""
        + "}"
        + "]"
        + "}";

    PdxInstance pdx = JSONFormatter.fromJSON(jsonCustomer);
    region.put(key, pdx);
    return pdx;
  }

  private void verifyQuery(String query, String defaultField, String ... expectedKeys) throws Exception {
    final LuceneQuery<String, Object> queryWithStandardAnalyzer = luceneService.createLuceneQueryFactory().create(
      INDEX_NAME, REGION_NAME, query, defaultField);

    verifyQueryKeys(queryWithStandardAnalyzer, expectedKeys);
  }
  
  private void verifyQuery(String query, String DEFAULT_FIELD, HashMap expectedResults) throws Exception {
    final LuceneQuery<String, Object> queryWithStandardAnalyzer = luceneService.createLuceneQueryFactory().create(
      INDEX_NAME, REGION_NAME, query, DEFAULT_FIELD);

    verifyQueryKeyAndValues(queryWithStandardAnalyzer, expectedResults);
  }

  private static class MyCharacterTokenizer extends CharTokenizer {
    @Override
    protected boolean isTokenChar(final int character) {
      return '_' != character;
    }
  }

  private static class MyCharacterAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(final String field) {
      Tokenizer tokenizer = new MyCharacterTokenizer();
      TokenStream filter = new LowerCaseFilter(tokenizer);
      return new TokenStreamComponents(tokenizer, filter);
    }
  }

}
