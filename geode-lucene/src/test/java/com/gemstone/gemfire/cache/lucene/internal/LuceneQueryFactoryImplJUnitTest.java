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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneQueryFactoryImplJUnitTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void shouldCreateQueryWithCorrectAttributes() {
    Cache cache = mock(Cache.class);
    Region region = mock(Region.class);
    when(cache.getRegion(any())).thenReturn(region);
    LuceneQueryFactoryImpl f = new LuceneQueryFactoryImpl(cache);
    f.setPageSize(5);
    f.setResultLimit(25);
    String[] projection = new String[] {"a", "b"};
    f.setProjectionFields(projection);
    LuceneQuery<Object, Object> query = f.create("index", "region", new StringQueryProvider("test"));
    assertEquals(25, query.getLimit());
    assertEquals(5, query.getPageSize());
    assertArrayEquals(projection, query.getProjectedFieldNames());
    
    Mockito.verify(cache).getRegion(Mockito.eq("region"));
  }

  @Test
  public void shouldThrowIllegalArgumentWhenRegionIsMissing() {
    Cache cache = mock(Cache.class);
    LuceneQueryFactoryImpl f = new LuceneQueryFactoryImpl(cache);
    thrown.expect(IllegalArgumentException.class);
    LuceneQuery<Object, Object> query = f.create("index", "region", new StringQueryProvider("test"));
  }

}
