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
package org.apache.geode.cache.lucene.internal;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.distributed.TopEntries;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PageableLuceneQueryResultsImplJUnitTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private List<EntryScore<String>> hits;
  private List<LuceneResultStruct> expected = new ArrayList<LuceneResultStruct>();
  private Region<String, String> userRegion;
  
  @Before
  public void setUp() {
    hits = new ArrayList<EntryScore<String>>();
    
    for(int i =0; i < 23; i++) {
      hits.add(new EntryScore("key_" + i, i));
      expected.add(new LuceneResultStructImpl<String, String>("key_" + i, "value_" + i, i));
    }
    
    userRegion = Mockito.mock(Region.class);
    
    Mockito.when(userRegion.getAll(Mockito.anyCollection())).thenAnswer(new Answer() {

      @Override
      public Map answer(InvocationOnMock invocation) throws Throwable {
        Collection<String> keys = invocation.getArgumentAt(0, Collection.class);
        Map<String, String> results = new HashMap<String, String>();
        for(String key : keys) {
          results.put(key, key.replace("key_", "value_"));
        }
        
        return results;
      }
    });
  }
  
  @Test
  public void testMaxStore() {
    hits.set(5, new EntryScore<String>("key_5", 502));
    
    PageableLuceneQueryResultsImpl<String, String> results = new PageableLuceneQueryResultsImpl<String, String>(hits, null, 5);
    
    assertEquals(502, results.getMaxScore(), 0.1f);
  }
  
  @Test
  public void testPagination() {
    PageableLuceneQueryResultsImpl<String, String> results = new PageableLuceneQueryResultsImpl<String, String>(hits, userRegion, 10);
    
    assertEquals(23, results.size());
    
    assertTrue(results.hasNext());
    
    List<LuceneResultStruct<String, String>> next  = results.next();
    assertEquals(expected.subList(0, 10), next);
    
    assertTrue(results.hasNext());
    next  = results.next();
    assertEquals(expected.subList(10, 20), next);
    
    assertTrue(results.hasNext());
    next  = results.next();
    assertEquals(expected.subList(20, 23), next);
    
    
    assertFalse(results.hasNext());
    thrown.expect(NoSuchElementException.class);
    results.next();
  }
  
  @Test
  public void testNoPagination() {
    PageableLuceneQueryResultsImpl<String, String> results = new PageableLuceneQueryResultsImpl<String, String>(hits, userRegion, 0);
    
    assertEquals(23, results.size());
    
    assertTrue(results.hasNext());
    
    List<LuceneResultStruct<String, String>> next  = results.next();
    assertEquals(expected, next);
    
    assertFalse(results.hasNext());
    thrown.expect(NoSuchElementException.class);
    results.next();
  }

  @Test
  public void shouldThrowNoSuchElementExceptionFromNextWithNoMorePages() {
    PageableLuceneQueryResultsImpl<String, String> results = new PageableLuceneQueryResultsImpl<>(
      Collections.emptyList(), userRegion, 0);

    assertFalse(results.hasNext());
    thrown.expect(NoSuchElementException.class);
    results.next();
  }

}
