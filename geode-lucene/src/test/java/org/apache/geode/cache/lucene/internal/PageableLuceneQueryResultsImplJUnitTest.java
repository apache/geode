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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class PageableLuceneQueryResultsImplJUnitTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private List<EntryScore<String>> hits;
  private final List<LuceneResultStruct> expected = new ArrayList<LuceneResultStruct>();
  private Region<String, String> userRegion;
  private Execution execution;

  @Before
  public void setUp() {
    hits = new ArrayList<EntryScore<String>>();

    for (int i = 0; i < 23; i++) {
      hits.add(new EntryScore("key_" + i, i));
      expected.add(new LuceneResultStructImpl<String, String>("key_" + i, "value_" + i, i));
    }

    userRegion = mock(Region.class);

    final ResultCollector collector = mock(ResultCollector.class);
    execution = mock(Execution.class);
    when(execution.withFilter(any())).thenReturn(execution);
    when(execution.withCollector(any())).thenReturn(execution);
    when(execution.execute(anyString())).thenReturn(collector);

    when(collector.getResult()).then(new Answer() {

      @Override
      public Map answer(InvocationOnMock invocation) throws Throwable {
        ArgumentCaptor<Set> captor = ArgumentCaptor.forClass(Set.class);
        verify(execution, atLeast(1)).withFilter(captor.capture());
        Collection<String> keys = captor.getValue();
        Map<String, String> results = new HashMap<String, String>();
        for (String key : keys) {
          results.put(key, key.replace("key_", "value_"));
        }

        return results;
      }
    });
  }

  @Test
  public void testMaxStore() {
    hits.set(5, new EntryScore<String>("key_5", 502));

    PageableLuceneQueryResultsImpl<String, String> results =
        new PageableLuceneQueryResultsImpl<String, String>(hits, null, 5);

    assertEquals(502, results.getMaxScore(), 0.1f);
  }

  @Test
  public void testPagination() {
    PageableLuceneQueryResultsImpl<String, String> results =
        new PageableLuceneQueryResultsImpl<String, String>(hits, userRegion, 10) {
          @Override
          protected Execution onRegion() {
            return execution;
          }
        };

    assertEquals(23, results.size());

    assertTrue(results.hasNext());

    List<LuceneResultStruct<String, String>> next = results.next();
    assertEquals(expected.subList(0, 10), next);

    assertTrue(results.hasNext());
    next = results.next();
    assertEquals(expected.subList(10, 20), next);

    assertTrue(results.hasNext());
    next = results.next();
    assertEquals(expected.subList(20, 23), next);


    assertFalse(results.hasNext());
    thrown.expect(NoSuchElementException.class);
    results.next();
  }

  @Test
  public void testNoPagination() {
    PageableLuceneQueryResultsImpl<String, String> results =
        new PageableLuceneQueryResultsImpl<String, String>(hits, userRegion, 0) {
          @Override
          protected Execution onRegion() {
            return execution;
          }
        };

    assertEquals(23, results.size());

    assertTrue(results.hasNext());

    List<LuceneResultStruct<String, String>> next = results.next();
    assertEquals(expected, next);

    assertFalse(results.hasNext());
    thrown.expect(NoSuchElementException.class);
    results.next();
  }

  @Test
  public void shouldThrowNoSuchElementExceptionFromNextWithNoMorePages() {
    PageableLuceneQueryResultsImpl<String, String> results =
        new PageableLuceneQueryResultsImpl<>(Collections.emptyList(), userRegion, 0);

    assertFalse(results.hasNext());
    thrown.expect(NoSuchElementException.class);
    results.next();
  }

}
