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
package org.apache.geode.cache.lucene.internal.distributed;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.DEFAULT_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.internal.StringQueryProvider;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneQueryFunctionContextJUnitTest {

  @Test
  public void testLuceneFunctionArgsDefaults() {
    LuceneFunctionContext<IndexResultCollector> context = new LuceneFunctionContext<>();
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, context.getLimit());
    assertEquals(DataSerializableFixedID.LUCENE_FUNCTION_CONTEXT, context.getDSFID());
  }

  @Test
  public void testSerialization() {
    LuceneQueryProvider provider = new StringQueryProvider("text", DEFAULT_FIELD);
    CollectorManager<TopEntriesCollector> manager = new TopEntriesCollectorManager("test");
    LuceneFunctionContext<TopEntriesCollector> context =
        new LuceneFunctionContext<>(provider, "testIndex", manager, 123);

    LuceneFunctionContext<TopEntriesCollector> copy = CopyHelper.deepCopy(context);
    assertEquals(123, copy.getLimit());
    assertNotNull(copy.getQueryProvider());
    assertEquals("text", ((StringQueryProvider) copy.getQueryProvider()).getQueryString());
    assertEquals(TopEntriesCollectorManager.class, copy.getCollectorManager().getClass());
    assertEquals("test", ((TopEntriesCollectorManager) copy.getCollectorManager()).getId());
    assertEquals("testIndex", copy.getIndexName());
  }
}
