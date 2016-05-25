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

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.verifyQueryKeys;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.lucene.test.TestObject;
import com.gemstone.gemfire.cache.query.QueryException;
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
  public void shouldNotTokenizeWordsWithKeywordAnalyzer() throws ParseException {
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
    verifyQuery("field1:\"one three\"", "A", "C");
    
    // standard analyzer will not tokenize by '_'
    // this query string will be parsed as "one_three"
    // query will be--field1:one_three
    verifyQuery("field1:one_three");
    
    // standard analyzer will tokenize by '@'
    // this query string will be parsed as "one" "three"
    // query will be--field1:one field1:three
    verifyQuery("field1:one@three", "A", "B", "C");
    
    // keyword analyzer, this query will only match the entry that exactly matches
    // this query string will be parsed as "one three"
    // but keyword analyzer will parse one@three to be "one three"
    // query will be--field2:one three
    verifyQuery("field2:\"one three\"", "A");

    // keyword analyzer without double quote. It should be the same as 
    // with double quote
    // query will be--field2:one@three
    verifyQuery("field2:one@three", "C");
  }

  @Test()
  public void shouldTokenizeUsingMyCharacterAnalyser() throws ParseException {
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

    verifyQuery("field1:one AND field2:two_four", "A");
    verifyQuery("field1:one AND field2:two", "A");
    verifyQuery("field1:three AND field2:four", "A");
  }

  @Test()
  public void shouldAllowNullInFieldValue() throws ParseException {
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

    verifyQuery("field1:one", "A");
  }

  @Test()
  public void throwFunctionExceptionWhenGivenBadQuery() {
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);

    //Create a query that throws an exception
    final LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
      index -> {
        throw new QueryException("Bad query");
      });


    thrown.expect(FunctionException.class);
    thrown.expectCause(isA(QueryException.class));
    try {
      query.search();
    } catch(FunctionException e) {
      assertEquals(QueryException.class, e.getCause().getClass());
      throw e;
    }

  }

  private void verifyQuery(String query, String ... expectedKeys) throws ParseException {
    final LuceneQuery<Object, Object> queryWithStandardAnalyzer = luceneService.createLuceneQueryFactory().create(
      INDEX_NAME, REGION_NAME, query);

    verifyQueryKeys(queryWithStandardAnalyzer, expectedKeys);
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
