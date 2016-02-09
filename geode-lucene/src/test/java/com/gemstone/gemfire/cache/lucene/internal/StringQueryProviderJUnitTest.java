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

import static org.junit.Assert.assertEquals;

import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StringQueryProviderJUnitTest {
  static final String INDEXED_REGION = "indexedRegion";

  LuceneIndex mockIndex;

  @Before
  public void initMocksAndCommonObjects() {
    mockIndex = Mockito.mock(LuceneIndex.class, "mockIndex");
    String[] fields = { "field-1", "field-2" };
    Mockito.doReturn(fields).when(mockIndex).getFieldNames();
    Mockito.doReturn("mockIndex").when(mockIndex).getName();
    Mockito.doReturn("mockRegionPath").when(mockIndex).getRegionPath();
  }

  @Test
  public void testQueryConstruction() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider("foo:bar");
    Query query = provider.getQuery(mockIndex);
    Assert.assertNotNull(query);
    assertEquals("foo:bar", query.toString());
  }

  @Test
  public void usesSearchableFieldsAsDefaults() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider("findThis");
    Query query = provider.getQuery(mockIndex);
    Assert.assertNotNull(query);
    assertEquals("field-1:findthis field-2:findthis", query.toString());
  }

  @Test
  @Ignore("Custom analyzer not yet supported, this is a duplicate test right now")
  public void usesCustomAnalyzer() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider("findThis");
    Query query = provider.getQuery(mockIndex);
    Assert.assertNotNull(query);
    assertEquals("field-1:findthis field-2:findthis", query.toString());
  }

  @Test(expected = QueryException.class)
  public void errorsOnMalformedQueryString() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider("invalid:lucene:query:string");
    provider.getQuery(mockIndex);
  }
  
  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    StringQueryProvider provider = new StringQueryProvider("text:search");
    StringQueryProvider copy = CopyHelper.deepCopy(provider);
    assertEquals("text:search", copy.getQueryString());
  }
}
