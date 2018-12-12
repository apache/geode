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
package org.apache.geode.cache.query.internal.index;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.cache.LocalRegion;

public class PrimaryKeyIndexTest {

  private PrimaryKeyIndex index;
  LocalRegion region = mock(LocalRegion.class);
  ExecutionContext context = mock(ExecutionContext.class);

  @Before
  public void setup() {
    when(region.getAttributes()).thenReturn(mock(RegionAttributes.class));
    index = new PrimaryKeyIndex(null, null, region, null,
        null, null, null, null, null, null);
  }

  @Test
  public void applyCqOrProjectionWhenContextNotForCqShouldAddValueToResults() throws Exception {
    when(context.getQuery()).thenReturn(mock(DefaultQuery.class));
    List results = new LinkedList();
    String value = "value";
    index.applyCqOrProjection(null, context, results, value, null, false, "key");
    assertEquals(value, results.get(0));
  }

  @Test
  public void applyCqOrProjectionWhenContextIsCqShouldAddCqEntryToResults() throws Exception {
    when(context.isCqQueryContext()).thenReturn(true);
    when(context.getQuery()).thenReturn(mock(DefaultQuery.class));
    List results = new LinkedList();
    String value = "value";
    index.applyCqOrProjection(null, context, results, value, null, false, "key");
    assertTrue(results.get(0) instanceof CqEntry);
  }

  @Test
  public void lockQueryWithoutProjectionWithMatchingResultForCqShouldReturnCorrectCqEvent()
      throws Exception {
    String value = "value";
    when(context.isCqQueryContext()).thenReturn(true);
    when(context.getQuery()).thenReturn(mock(DefaultQuery.class));
    Region.Entry entry = mock(Region.Entry.class);
    when(entry.getValue()).thenReturn(value);
    when(region.accessEntry(eq("key"), anyBoolean())).thenReturn(entry);
    List results = new LinkedList();
    index.lockedQuery("key", OQLLexerTokenTypes.TOK_EQ, results, null, context);
    assertTrue(results.get(0) instanceof CqEntry);
  }

  @Test
  public void lockQueryWithoutProjectionWithMatchingResultShouldReturnCorrectValue()
      throws Exception {
    String value = "value";
    when(context.getQuery()).thenReturn(mock(DefaultQuery.class));
    Region.Entry entry = mock(Region.Entry.class);
    when(entry.getValue()).thenReturn(value);
    when(region.accessEntry(eq("key"), anyBoolean())).thenReturn(entry);
    List results = new LinkedList();
    index.lockedQuery("key", OQLLexerTokenTypes.TOK_EQ, results, null, context);
    assertEquals(value, results.get(0));
  }


}
