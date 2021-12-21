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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.DEFAULT_FIELD;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.IntRangeQueryProvider;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.verifyQueryKeyAndValues;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.verifyQueryKeys;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.phonetic.DoubleMetaphoneFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.test.TestObject;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * This class contains tests of lucene queries that can fit
 */
@Category({LuceneTest.class})
public class LuceneQueriesIntegrationTest extends LuceneIntegrationTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  protected static final String INDEX_NAME = "index";
  public static final String REGION_NAME = "index";
  protected Region region;

  protected Region createRegionAndIndex(Map<String, Analyzer> fields) {
    ((LuceneIndexFactoryImpl) luceneService.createIndexFactory().setFields(fields))
        .create(INDEX_NAME, REGION_NAME, LuceneServiceImpl.LUCENE_REINDEX);
    region = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
    return region;
  }

  protected Region createRegionAndIndex(RegionShortcut regionShortcut, String... fields) {
    ((LuceneIndexFactoryImpl) luceneService.createIndexFactory().setFields(fields))
        .create(INDEX_NAME, REGION_NAME, LuceneServiceImpl.LUCENE_REINDEX);
    region = cache.createRegionFactory(regionShortcut).create(REGION_NAME);
    return region;
  }

  @Test()
  public void shouldNotTokenizeWordsWithKeywordAnalyzer() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("field1", new StandardAnalyzer());
    fields.put("field2", new KeywordAnalyzer());
    region = createRegionAndIndex(fields);

    // Put two values with some of the same tokens
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

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

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
  public void shouldQueryUsingIntRangeQueryProvider() throws Exception {
    // Note: range query on numeric field has some limitations. But IntRangeQueryProvider
    // provided basic functionality
    region = createRegionAndIndex(RegionShortcut.PARTITION, LuceneService.REGION_VALUE_FIELD);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    region.put("primitiveInt0", 122);
    region.put("primitiveInt1", 123);
    region.put("primitiveInt2", 223);
    region.put("primitiveInt3", 224);

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
    verifyQueryUsingCustomizedProvider(LuceneService.REGION_VALUE_FIELD, 123, 223, "primitiveInt1",
        "primitiveInt2");
  }

  @Test()
  public void shouldQueryUsingSoundexAnalyzer() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("field1", new StandardAnalyzer());
    fields.put("field2", new DoubleMetaphoneAnalyzer());
    region = createRegionAndIndex(fields);

    // Put two values with some of the same tokens
    String value1 = "Stefan";
    String value2 = "Steph";
    String value3 = "Stephen";
    String value4 = "Steve";
    String value5 = "Steven";
    String value6 = "Stove";
    String value7 = "Stuffin";

    region.put("A", new TestObject(value1, value1));
    region.put("B", new TestObject(value2, value2));
    region.put("C", new TestObject(value3, value3));
    region.put("D", new TestObject(value4, value4));
    region.put("E", new TestObject(value5, value5));
    region.put("F", new TestObject(value6, value6));
    region.put("G", new TestObject(value7, value7));

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
    // soundex search
    verifyQuery("field2:Stephen", DEFAULT_FIELD, "A", "C", "E", "G");

    // compare with Ste* search on soundex analyzer will not find anything
    // but on standard analyzer will find 5 matchs
    verifyQuery("field2:Ste*", DEFAULT_FIELD);
    verifyQuery("field1:Ste*", DEFAULT_FIELD, "A", "B", "C", "D", "E");
  }

  @Ignore
  @Test()
  public void queryParserCannotQueryByRange() throws Exception {
    // Note: range query on numeric field has some limitations. But IntRangeQueryProvider
    // provided basic functionality
    region = createRegionAndIndex(RegionShortcut.PARTITION, LuceneService.REGION_VALUE_FIELD);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    region.put("primitiveInt0", 122);
    region.put("primitiveInt1", 123);
    region.put("primitiveInt2", 223);
    region.put("primitiveInt3", 224);

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    // Note: current QueryParser cannot query by range. It's a known issue in lucene
    verifyQuery(LuceneService.REGION_VALUE_FIELD + ":[123 TO 223]",
        LuceneService.REGION_VALUE_FIELD);

    region.put("primitiveDouble1", 123.0);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    thrown.expectMessage("java.lang.IllegalArgumentException");
    verifyQueryUsingCustomizedProvider(LuceneService.REGION_VALUE_FIELD, 123, 223, "primitiveInt1",
        "primitiveInt2");
  }

  @Test()
  public void shouldPaginateResults() throws Exception {

    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(2);

    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    assertTrue(pages.hasNext());
    assertEquals(7, pages.size());
    final List<LuceneResultStruct<Object, Object>> page1 = pages.next();
    final List<LuceneResultStruct<Object, Object>> page2 = pages.next();
    final List<LuceneResultStruct<Object, Object>> page3 = pages.next();
    final List<LuceneResultStruct<Object, Object>> page4 = pages.next();
    List<LuceneResultStruct<Object, Object>> allEntries = new ArrayList<>();
    allEntries.addAll(page1);
    allEntries.addAll(page2);
    allEntries.addAll(page3);
    allEntries.addAll(page4);

    assertEquals(region.keySet(),
        allEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet()));
    assertEquals(region.values(),
        allEntries.stream().map(entry -> entry.getValue()).collect(Collectors.toSet()));
  }

  @Test
  public void shouldReturnValuesFromFindValues() throws Exception {
    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(2);
    assertEquals(region.values(), new HashSet(query.findValues()));
  }

  private LuceneQuery<Object, Object> addValuesAndCreateQuery(int pagesize)
      throws InterruptedException {
    region = createRegionAndIndex(RegionShortcut.PARTITION, "field1", "field2");
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    // Put two values with some of the same tokens
    String value1 = "one three";
    String value2 = "one two three";
    String value3 = "one@three";
    region.put("A", new TestObject(value1, value1));
    region.put("B", new TestObject(value2, value2));
    region.put("C", new TestObject(value3, value3));
    region.put("D", new TestObject(value1, value1));
    region.put("E", new TestObject(value2, value2));
    region.put("F", new TestObject(value3, value3));
    region.put("G", new TestObject(value1, value2));

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);
    return luceneService.createLuceneQueryFactory().setPageSize(pagesize).create(INDEX_NAME,
        REGION_NAME, "one", "field1");
  }

  @Test()
  public void shouldTokenizeUsingMyCharacterAnalyser() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    // not to specify field1's analyzer, it should use standard analyzer
    // Note: fields has to contain "field1", otherwise, field1 will not be tokenized
    fields.put("field1", null);
    fields.put("field2", new MyCharacterAnalyzer());
    region = createRegionAndIndex(fields);

    // Put two values with some of the same tokens
    String value1 = "one three";
    String value4 = "two_four";
    String value3 = "two@four";
    region.put("A", new TestObject(value1, value4));
    region.put("B", new TestObject(value1, value3));
    region.put("C", new TestObject(value3, value3));
    region.put("D", new TestObject(value4, value4));

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    verifyQuery("field1:one AND field2:two_four", DEFAULT_FIELD, "A");
    verifyQuery("field1:one AND field2:two", DEFAULT_FIELD, "A");
    verifyQuery("field1:three AND field2:four", DEFAULT_FIELD, "A");
  }

  @Test()
  public void shouldAllowNullInFieldValue() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("field1", null);
    fields.put("field2", null);
    region = createRegionAndIndex(fields);

    // Put two values with some of the same tokens
    String value1 = "one three";
    region.put("A", new TestObject(value1, null));
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    verifyQuery("field1:one", DEFAULT_FIELD, "A");
  }

  @Test()
  public void queryJsonObject() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("name", null);
    fields.put("lastName", null);
    fields.put("address", null);
    region = createRegionAndIndex(fields);

    // Put two values with some of the same tokens
    PdxInstance pdx1 = insertAJson(region, "jsondoc1");
    PdxInstance pdx2 = insertAJson(region, "jsondoc2");
    PdxInstance pdx10 = insertAJson(region, "jsondoc10");
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    HashMap expectedResults = new HashMap();
    expectedResults.put("jsondoc1", pdx1);
    expectedResults.put("jsondoc10", pdx10);
    verifyQuery("name:jsondoc1*", DEFAULT_FIELD, expectedResults);
  }

  @Test()
  public void waitUntilFlushThrowsIllegalStateExceptionWhenAEQNotFound() throws Exception {
    Map<String, Analyzer> fields = new HashMap<>();
    fields.put("name", null);
    fields.put("lastName", null);
    fields.put("address", null);
    region = createRegionAndIndex(fields);

    // This is to send IllegalStateException from WaitUntilFlushedFunction
    String nonCreatedIndex = "index2";
    boolean result = false;
    try {
      result = luceneService.waitUntilFlushed(nonCreatedIndex, REGION_NAME, 60000,
          TimeUnit.MILLISECONDS);
      fail(
          "Should have got the exception because the queue does not exist for the non created index ");
    } catch (Exception ex) {
      assertEquals(ex.getMessage(),
          "java.lang.IllegalStateException: The AEQ does not exist for the index index2 region "
              + SEPARATOR + "index");
      assertFalse(result);
    }
  }

  @Test()
  public void shouldAllowQueryOnRegionWithStringValue() throws Exception {
    region = createRegionAndIndex(RegionShortcut.PARTITION, LuceneService.REGION_VALUE_FIELD);
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);

    region.put("A", "one three");
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    verifyQuery("one", LuceneService.REGION_VALUE_FIELD, "A");
  }

  @Test()
  public void throwFunctionExceptionWhenGivenBadQuery() throws Exception {
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    region = createRegionAndIndex(RegionShortcut.PARTITION, "text");

    // Create a query that throws an exception
    final LuceneQuery<Object, Object> query =
        luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, (index) -> {
          throw new LuceneQueryException("Bad query");
        });


    thrown.expect(LuceneQueryException.class);
    query.findPages();
  }

  @Test
  public void shouldReturnAllResultsWhenPaginationIsDisabled() throws Exception {
    // Pagination disabled by setting page size = 0.
    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(0);
    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    assertTrue(pages.hasNext());
    assertEquals(7, pages.size());
    final List<LuceneResultStruct<Object, Object>> page = pages.next();
    assertFalse(pages.hasNext());
    assertEquals(region.keySet(),
        page.stream().map(entry -> entry.getKey()).collect(Collectors.toSet()));
    assertEquals(region.values(),
        page.stream().map(entry -> entry.getValue()).collect(Collectors.toSet()));
  }

  @Test
  public void shouldReturnCorrectResultsOnDeletionAfterQueryExecution() throws Exception {
    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(2);
    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    List<LuceneResultStruct<Object, Object>> allEntries = new ArrayList<>();
    assertTrue(pages.hasNext());
    assertEquals(7, pages.size());
    // Destroying an entry from the region after the query is executed.
    region.destroy("C");
    final List<LuceneResultStruct<Object, Object>> page1 = pages.next();
    assertEquals(2, page1.size());
    final List<LuceneResultStruct<Object, Object>> page2 = pages.next();
    assertEquals(2, page2.size());
    final List<LuceneResultStruct<Object, Object>> page3 = pages.next();
    assertEquals(2, page3.size());
    assertFalse(pages.hasNext());

    allEntries.addAll(page1);
    allEntries.addAll(page2);
    allEntries.addAll(page3);
    assertEquals(region.keySet(),
        allEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet()));
    assertEquals(region.values(),
        allEntries.stream().map(entry -> entry.getValue()).collect(Collectors.toSet()));
  }

  @Test
  public void shouldReturnCorrectResultsOnDeletionAfterQueryExecutionWithLoader() throws Exception {
    final int pageSize = 2;
    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(pageSize);
    region.getAttributesMutator().setCacheLoader(new CacheLoader() {
      @Override
      public Object load(final LoaderHelper helper) throws CacheLoaderException {
        return new TestObject("should not", "load this");
      }

      @Override
      public void close() {

      }
    });
    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    List<LuceneResultStruct<Object, Object>> allEntries = new ArrayList<>();
    assertTrue(pages.hasNext());
    assertEquals(7, pages.size());
    // Destroying an entry from the region after the query is executed.
    region.destroy("C");
    final List<LuceneResultStruct<Object, Object>> page1 = pages.next();
    assertEquals(pageSize, page1.size());
    final List<LuceneResultStruct<Object, Object>> page2 = pages.next();
    assertEquals(pageSize, page2.size());
    final List<LuceneResultStruct<Object, Object>> page3 = pages.next();
    assertEquals(pageSize, page3.size());
    assertFalse(pages.hasNext());

    allEntries.addAll(page1);
    allEntries.addAll(page2);
    allEntries.addAll(page3);
    assertEquals(region.keySet(),
        allEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet()));
    assertEquals(region.values(),
        allEntries.stream().map(entry -> entry.getValue()).collect(Collectors.toSet()));
  }

  @Test
  public void shouldReturnCorrectResultsOnMultipleDeletionsAfterQueryExecution() throws Exception {
    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(2);

    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    List<LuceneResultStruct<Object, Object>> allEntries = new ArrayList<>();

    assertTrue(pages.hasNext());
    assertEquals(7, pages.size());

    // Destroying an entry from the region after the query is executed.
    region.destroy("C");
    allEntries.addAll(pages.next());

    // Destroying an entry from allEntries and the region after it is fetched through pages.next().
    Object removeKey = allEntries.remove(0).getKey();
    region.destroy(removeKey);
    allEntries.addAll(pages.next());

    // Destroying a region entry which has't been fetched through pages.next() yet.
    Set resultKeySet = allEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet());

    for (Object key : region.keySet()) {
      if (!resultKeySet.contains(key)) {
        region.destroy(key);
        break;
      }
    }

    allEntries.addAll(pages.next());
    assertFalse(pages.hasNext());

    assertEquals(region.keySet(),
        allEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet()));
    assertEquals(region.values(),
        allEntries.stream().map(entry -> entry.getValue()).collect(Collectors.toSet()));
  }

  @Test
  public void shouldReturnCorrectResultsOnAllDeletionsAfterQueryExecution() throws Exception {
    final LuceneQuery<Object, Object> query = addValuesAndCreateQuery(2);

    final PageableLuceneQueryResults<Object, Object> pages = query.findPages();
    assertTrue(pages.hasNext());
    assertEquals(7, pages.size());
    region.destroy("A");
    region.destroy("B");
    region.destroy("C");
    region.destroy("D");
    region.destroy("E");
    region.destroy("F");
    region.destroy("G");
    assertTrue(pages.hasNext());
    final List<LuceneResultStruct<Object, Object>> page1 = pages.next();
    assertEquals(2, page1.size());
    assertFalse(pages.hasNext());

  }

  @Test()
  public void soundexQueryReturnExpectedTruePositiveAndFalsePositive() throws Exception {
    Map<String, Analyzer> fields = new HashMap<String, Analyzer>();
    fields.put("field1", new DoubleMetaphoneAnalyzer());
    fields.put("field2", null);
    region = createRegionAndIndex(fields);

    region.put("A", new TestObject("Stefan", "soundex"));
    region.put("B", new TestObject("Steph", "soundex"));
    region.put("C", new TestObject("Stephen", "soundex"));
    region.put("D", new TestObject("Steve", "soundex"));
    region.put("E", new TestObject("Steven", "soundex"));
    region.put("F", new TestObject("Stove", "soundex"));
    region.put("G", new TestObject("Stuffin", "soundex"));
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    verifyQuery("field1:Stephen", DEFAULT_FIELD, "A", "C", "E", "G");
  }

  private PdxInstance insertAJson(Region region, String key) {
    String jsonCustomer = "{" + "\"name\": \"" + key + "\"," + "\"lastName\": \"Smith\","
        + " \"age\": 25," + "\"address\":" + "{" + "\"streetAddress\": \"21 2nd Street\","
        + "\"city\": \"New York\"," + "\"state\": \"NY\"," + "\"postalCode\": \"10021\"" + "},"
        + "\"phoneNumber\":" + "[" + "{" + " \"type\": \"home\"," + "\"number\": \"212 555-1234\""
        + "}," + "{" + " \"type\": \"fax\"," + "\"number\": \"646 555-4567\"" + "}" + "]" + "}";

    PdxInstance pdx = JSONFormatter.fromJSON(jsonCustomer);
    region.put(key, pdx);
    return pdx;
  }

  private void verifyQueryUsingCustomizedProvider(String fieldName, int lowerValue, int upperValue,
      String... expectedKeys) throws Exception {
    IntRangeQueryProvider provider = new IntRangeQueryProvider(fieldName, lowerValue, upperValue);
    LuceneQuery<String, Object> queryWithCustomizedProvider =
        luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, provider);

    verifyQueryKeys(queryWithCustomizedProvider, expectedKeys);
  }

  private void verifyQuery(String query, String defaultField, String... expectedKeys)
      throws Exception {
    final LuceneQuery<String, Object> queryWithStandardAnalyzer = luceneService
        .createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, query, defaultField);

    verifyQueryKeys(queryWithStandardAnalyzer, expectedKeys);
  }

  private void verifyQuery(String query, String DEFAULT_FIELD, HashMap expectedResults)
      throws Exception {
    final LuceneQuery<String, Object> queryWithStandardAnalyzer = luceneService
        .createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, query, DEFAULT_FIELD);

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

  private static class DoubleMetaphoneAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(final String field) {
      Tokenizer tokenizer = new StandardTokenizer();
      TokenStream stream = new DoubleMetaphoneFilter(tokenizer, 6, false);
      return new TokenStreamComponents(tokenizer, stream);
    }
  }
}
