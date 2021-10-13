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

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.DEFAULT_FIELD;
import static org.junit.Assert.assertEquals;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class StringQueryProviderJUnitTest {

  private LuceneIndexImpl mockIndex;

  @Before
  public void initMocksAndCommonObjects() {
    mockIndex = Mockito.mock(LuceneIndexImpl.class, "mockIndex");
    String[] fields = {"field-1", "field-2"};
    Analyzer analyzer = new StandardAnalyzer();
    Mockito.doReturn(analyzer).when(mockIndex).getAnalyzer();
    Mockito.doReturn(fields).when(mockIndex).getFieldNames();
    Mockito.doReturn("mockIndex").when(mockIndex).getName();
    Mockito.doReturn("mockRegionPath").when(mockIndex).getRegionPath();
  }

  @Test
  public void testQueryConstruction() throws LuceneQueryException {
    StringQueryProvider provider = new StringQueryProvider("foo:bar", DEFAULT_FIELD);
    Query query = provider.getQuery(mockIndex);
    Assert.assertNotNull(query);
    assertEquals("foo:bar", query.toString());
  }

  @Test
  @Ignore("Custom analyzer not yet supported, this is a duplicate test right now")
  public void usesCustomAnalyzer() throws LuceneQueryException {
    StringQueryProvider provider = new StringQueryProvider("findThis", DEFAULT_FIELD);
    Query query = provider.getQuery(mockIndex);
    Assert.assertNotNull(query);
    assertEquals("field-1:findthis field-2:findthis", query.toString());
  }

  @Test(expected = LuceneQueryException.class)
  public void errorsOnMalformedQueryString() throws LuceneQueryException {
    StringQueryProvider provider =
        new StringQueryProvider("invalid:lucene:query:string", DEFAULT_FIELD);
    provider.getQuery(mockIndex);
  }

  @Test
  public void testSerialization() {
    StringQueryProvider provider = new StringQueryProvider("text:search", DEFAULT_FIELD);
    StringQueryProvider copy = CopyHelper.deepCopy(provider);
    assertEquals("text:search", copy.getQueryString());
  }

  @Test
  public void defaultFieldParameterShouldBeUsedByQuery() throws LuceneQueryException {
    StringQueryProvider provider = new StringQueryProvider("findThis", "field-2");
    Query query = provider.getQuery(mockIndex);
    Assert.assertNotNull(query);
    assertEquals("field-2:findthis", query.toString());
  }

}
