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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LuceneQueryImplJUnitTest {
  private static int LIMIT = 123;
  private Cache cache;
  private Region<Object, Object> region;

  @Before
  public void createCache() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
  }

  @After
  public void removeCache() {
    FunctionService.unregisterFunction(LuceneFunction.ID);
    cache.close();
  }

  @Test
  public void test() {
    // Register a fake function to observe the function invocation
    FunctionService.unregisterFunction(LuceneFunction.ID);
    TestLuceneFunction function = new TestLuceneFunction();
    FunctionService.registerFunction(function);

    StringQueryProvider provider = new StringQueryProvider();
    LuceneQueryImpl<Object, Object> query = new LuceneQueryImpl<>("index", region, provider, null, LIMIT, 20);
    LuceneQueryResults<Object, Object> results = query.search();

    assertTrue(function.wasInvoked);
    assertEquals(2f * LIMIT, results.getMaxScore(), 0.01);
    int resultCount = 0;
    while (results.hasNextPage()) {
      List<LuceneResultStruct<Object, Object>> nextPage = results.getNextPage();
      resultCount += nextPage.size();
      if (results.hasNextPage()) {
        assertEquals(20, nextPage.size());
      }
    }
    assertEquals(LIMIT, resultCount);

    LuceneFunctionContext<? extends IndexResultCollector> funcArgs = function.args;
    assertEquals(provider.getQueryString(), ((StringQueryProvider) funcArgs.getQueryProvider()).getQueryString());
    assertEquals("index", funcArgs.getIndexName());
    assertEquals(LIMIT, funcArgs.getLimit());
  }

  private static class TestLuceneFunction extends FunctionAdapter {
    private static final long serialVersionUID = 1L;
    private boolean wasInvoked;
    private LuceneFunctionContext<? extends IndexResultCollector> args;

    @Override
    public void execute(FunctionContext context) {
      this.args = (LuceneFunctionContext<?>) context.getArguments();
      TopEntriesCollectorManager manager = (TopEntriesCollectorManager) args.getCollectorManager();

      assertEquals(LIMIT, manager.getLimit());

      wasInvoked = true;
      TopEntriesCollector lastResult = new TopEntriesCollector(null, 2 * LIMIT);
      // put more than LIMIT entries. The resultCollector should trim the results
      for (int i = LIMIT * 2; i >= 0; i--) {
        lastResult.collect(i, i * 1f);
      }
      assertEquals(LIMIT * 2, lastResult.getEntries().getHits().size());

      context.getResultSender().lastResult(lastResult);
    }

    @Override
    public String getId() {
      return LuceneFunction.ID;
    }
  }
}
