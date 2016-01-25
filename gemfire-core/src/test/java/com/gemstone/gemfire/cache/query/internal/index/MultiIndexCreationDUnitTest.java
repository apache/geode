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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class MultiIndexCreationDUnitTest extends CacheTestCase {

  private final String regionName = "MultiIndexCreationDUnitTest";
  public static volatile boolean hooked = false;

  public MultiIndexCreationDUnitTest(String name) {
    super(name);
  }

  public void testConcurrentMultiIndexCreationAndQuery() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);

    final int numberOfEntries = 10;
    final String name = "/" + regionName;

    // Start server1
    AsyncInvocation a1 = server1.invokeAsync(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);

        for (int i = 0; i < numberOfEntries; i++) {
          Portfolio p = new Portfolio(i);
          r.put("key-" + i, p);
        }

        IndexManager.testHook = new MultiIndexCreationTestHook();

        QueryService qs = getCache().getQueryService();
        qs.defineIndex("statusIndex", "status", r.getFullPath());
        qs.defineIndex("IDIndex", "ID", r.getFullPath());
        List<Index> indexes = qs.createDefinedIndexes();

        assertEquals("Only 2 indexes should have been created. ", 2,
            indexes.size());

        return null;
      }
    });

    final String[] queries = {
        "select * from " + name + " where status = 'active'",
        "select * from " + name + " where ID > 4" };

    AsyncInvocation a2 = server1.invokeAsync(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        long giveupTime = System.currentTimeMillis() + 60000;
        while (!hooked && System.currentTimeMillis() < giveupTime) {
          getLogWriter().info("Query Waiting for index hook.");
          pause(100);
        }
        assertTrue(hooked);
        
        QueryObserver old = QueryObserverHolder
            .setInstance(new QueryObserverAdapter() {
              private boolean indexCalled = false;

              public void afterIndexLookup(Collection results) {
                indexCalled = true;
              }

              public void endQuery() {
                assertFalse("Index should not have been used. ", indexCalled);
              }

            });

        SelectResults sr = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            sr = (SelectResults) getCache().getQueryService()
                .newQuery(queries[i]).execute();
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
    
    DistributedTestCase.join(a1, 6000, this.getLogWriter());
    
    if(a1.exceptionOccurred()) {
      fail(a1.getException().getMessage());
    }
    DistributedTestCase.join(a2, 6000, this.getLogWriter());
    if(a2.exceptionOccurred()) {
      fail(a2.getException().getMessage());
    }
    
    server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        IndexManager.testHook = null;
        QueryObserver old = QueryObserverHolder
            .setInstance(new QueryObserverAdapter() {
              private boolean indexCalled = false;

              public void afterIndexLookup(Collection results) {
                indexCalled = true;
              }

              public void endQuery() {
                assertTrue("Index should have been used. ", indexCalled);
              }

            });

        SelectResults sr = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            sr = (SelectResults) getCache().getQueryService()
                .newQuery(queries[i]).execute();
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
  public void tearDown2() throws Exception {
    hooked = false;
    invokeInEveryVM(CacheTestCase.class, "disconnectFromDS");
    super.tearDown2();
    invokeInEveryVM(QueryObserverHolder.class, "reset");
  }

  private static class MultiIndexCreationTestHook implements TestHook {

    @Override
    public void hook(int spot) throws RuntimeException {
      long giveupTime = System.currentTimeMillis() + 60000;
      if (spot == 13) {
        hooked = true;
        getLogWriter().info("MultiIndexCreationTestHook is hooked in create defined indexes.");
        while (hooked && System.currentTimeMillis() < giveupTime) {
          getLogWriter().info("MultiIndexCreationTestHook waiting.");
          pause(100);
        }
        assertEquals(hooked, false);
      }
    }
  }
}
