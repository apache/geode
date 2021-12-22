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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager.TestHook;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class MultiIndexCreationDUnitTest extends JUnit4CacheTestCase {

  private final String regionName = "MultiIndexCreationDUnitTest";
  public static volatile boolean hooked = false;

  public MultiIndexCreationDUnitTest() {
    super();
  }

  @Test
  public void testConcurrentMultiIndexCreationAndQuery() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);

    final int numberOfEntries = 10;
    final String name = SEPARATOR + regionName;

    // Start server1
    AsyncInvocation a1 = server1.invokeAsync(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        for (int i = 0; i < numberOfEntries; i++) {
          Portfolio p = new Portfolio(i);
          r.put("key-" + i, p);
        }

        IndexManager.testHook = new MultiIndexCreationTestHook();

        QueryService qs = getCache().getQueryService();
        qs.defineIndex("statusIndex", "status", r.getFullPath());
        qs.defineIndex("IDIndex", "ID", r.getFullPath());
        List<Index> indexes = qs.createDefinedIndexes();

        assertEquals("Only 2 indexes should have been created. ", 2, indexes.size());

        return null;
      }
    });

    final String[] queries = {"select * from " + name + " where status = 'active'",
        "select * from " + name + " where ID > 4"};

    AsyncInvocation a2 = server1.invokeAsync(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        long giveupTime = System.currentTimeMillis() + 60000;
        while (!hooked && System.currentTimeMillis() < giveupTime) {
          LogWriterUtils.getLogWriter().info("Query Waiting for index hook.");
          Wait.pause(100);
        }
        assertTrue(hooked);

        QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          @Override
          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          @Override
          public void endQuery() {
            assertFalse("Index should not have been used. ", indexCalled);
          }

        });

        SelectResults sr = null;
        for (final String query : queries) {
          try {
            sr = (SelectResults) getCache().getQueryService().newQuery(query).execute();
          } catch (Exception e) {
            fail("QueryExecution failed, " + e);
          }
          assertEquals(5, sr.size());
        }
        QueryObserverHolder.setInstance(old);

        hooked = false;

        return null;
      }
    });

    ThreadUtils.join(a1, 120000);

    if (a1.exceptionOccurred()) {
      fail(a1.getException().getMessage());
    }
    ThreadUtils.join(a2, 120000);
    if (a2.exceptionOccurred()) {
      fail(a2.getException().getMessage());
    }

    server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        IndexManager.testHook = null;
        QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          @Override
          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          @Override
          public void endQuery() {
            assertTrue("Index should have been used. ", indexCalled);
          }

        });

        SelectResults sr = null;
        for (final String query : queries) {
          try {
            sr = (SelectResults) getCache().getQueryService().newQuery(query).execute();
          } catch (Exception e) {
            fail("QueryExecution failed, " + e);
          }
          assertEquals(5, sr.size());
        }
        QueryObserverHolder.setInstance(old);
        return null;
      }
    });
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    hooked = false;
    Invoke.invokeInEveryVM(JUnit4DistributedTestCase::disconnectFromDS);
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(QueryObserverHolder::reset);
  }

  private static class MultiIndexCreationTestHook implements TestHook {

    @Override
    public void hook(int spot) throws RuntimeException {
      long giveupTime = System.currentTimeMillis() + 60000;
      if (spot == 13) {
        hooked = true;
        LogWriterUtils.getLogWriter()
            .info("MultiIndexCreationTestHook is hooked in create defined indexes.");
        while (hooked && System.currentTimeMillis() < giveupTime) {
          LogWriterUtils.getLogWriter().info("MultiIndexCreationTestHook waiting.");
          Wait.pause(100);
        }
        assertEquals(hooked, false);
      }
    }
  }
}
