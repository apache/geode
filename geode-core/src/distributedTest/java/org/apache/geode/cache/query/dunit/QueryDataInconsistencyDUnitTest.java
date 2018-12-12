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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * This tests the data inconsistency during update on an index and querying the same UNLOCKED index.
 */
@Category({OQLIndexTest.class})
public class QueryDataInconsistencyDUnitTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final int cnt = 0;
  private static final int cntDest = 10;
  private static VM server = null;
  private static String PartitionedRegionName1 = "TestPartitionedRegion1"; // default name
  private static String repRegionName = "TestRepRegion"; // default name
  private static volatile boolean hooked = false;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(1);

  @Rule
  public CacheRule cacheRule = new CacheRule(1);

  @Before
  public void setUp() {
    server = getVM(0);
    server.invoke(() -> cacheRule.createCache());
  }

  @After
  public void tearDown() {
    Invoke.invokeInEveryVM(QueryObserverHolder::reset);
  }

  @Test
  public void testCompactRangeIndex() {
    // Create caches
    server.invoke("create indexes", () -> {
      Cache cache = cacheRule.getCache();
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);

      // Create common Portflios and NewPortfolios
      for (int j = cnt; j < cntDest; j++) {
        region.put(new Integer(j), new Portfolio(j));
      }

      QueryService queryService = cache.getQueryService();
      try {
        Index index = queryService.createIndex("idIndex", "ID", "/" + repRegionName);
        assertEquals(10, index.getStatistics().getNumberOfKeys());
      } catch (Exception e) {
        logger.error(e);
        fail("Index creation failed");
      }
    });
    // Invoke update from client and stop in updateIndex
    // firesultSett before updating the RegionEntry and second after updating
    // the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync("update a Region Entry", () -> {
      Region repRegion = cacheRule.getCache().getRegion(repRegionName);
      IndexManager.testHook = new IndexManagerTestHook();
      repRegion.put(new Integer("1"), new Portfolio(cntDest + 1));
      // above call must be hooked in BEFORE_UPDATE_OP call.
    });
    server.invoke("query on server", () -> {
      QueryService queryService = cacheRule.getCache().getQueryService();
      await().until(() -> hooked);
      Object resultSet = null;
      try {
        resultSet = queryService
            .newQuery("<trace> select * from /" + repRegionName + " where ID = 1").execute();
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server.");
        IndexManager.testHook = null;
      }
      assertTrue(resultSet instanceof SelectResults);
      assertEquals(1, ((SelectResults) resultSet).size());
      Portfolio p1 = (Portfolio) ((SelectResults) resultSet).asList().get(0);
      if (p1.getID() != 1) {
        fail("Query thread did not verify index results even when RE is under update");
        IndexManager.testHook = null;
      }
      hooked = false;// Let client put go further.
    });

    // Client put is again hooked in AFTER_UPDATE_OP call in updateIndex.
    server.invoke("query on server", () -> {
      QueryService queryService = cacheRule.getCache().getQueryService();
      await().until(() -> hooked);
      Object resultSet = null;
      try {
        resultSet = queryService
            .newQuery("<trace> select * from /" + repRegionName + " where ID = 1").execute();
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server." + e.getMessage());
      } finally {
        IndexManager.testHook = null;
      }
      assertTrue(resultSet instanceof SelectResults);
      if (((SelectResults) resultSet).size() > 0) {
        Portfolio p1 = (Portfolio) ((SelectResults) resultSet).iterator().next();
        if (p1.getID() != 1) {
          fail("Query thread did not verify index results even when RE is under update and "
              + "RegionEntry value has been modified before releasing the lock");
          IndexManager.testHook = null;
        }
      }
      hooked = false;// Let client put go further.
    });
    await().until(joinThread(putThread));
  }

  @Test
  public void testRangeIndex() {
    // Create caches
    server.invoke("create indexes", () -> {
      Cache cache = cacheRule.getCache();
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
      IndexManager.testHook = null;
      // Create common Portfolios and NewPortfolios
      Position.cnt = 0;
      for (int j = cnt; j < cntDest; j++) {
        Portfolio p = new Portfolio(j);
        cache.getLogger().fine("Shobhit: portfolio " + j + " : " + p);
        region.put(j, p);
      }

      QueryService queryService = cache.getQueryService();
      try {
        Index index = queryService.createIndex("posIndex", "pos.secId",
            "/" + repRegionName + " p, p.positions.values pos");
        assertEquals(12, index.getStatistics().getNumberOfKeys());
      } catch (Exception e) {
        logger.error(e);
        fail("Index creation failed");
      }
    });
    // Invoke update from client and stop in updateIndex
    // firesultSett before updating the RegionEntry and second after updating
    // the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync("update a Region Entry", () -> {
      Cache cache = cacheRule.getCache();
      Region repRegion = cache.getRegion(repRegionName);
      IndexManager.testHook = new IndexManagerTestHook();
      Portfolio newPort = new Portfolio(cntDest + 1);
      cache.getLogger().fine("Shobhit: New Portfolio" + newPort);
      repRegion.put(new Integer("1"), newPort);
      // above call must be hooked in BEFORE_UPDATE_OP call.
    });

    server.invoke("query on server", () -> {
      Cache cache = cacheRule.getCache();
      QueryService queryService = cache.getQueryService();
      Position pos1 = null;
      await().until(() -> hooked);
      try {
        Object resultSet = queryService.newQuery("<trace> select pos from /" + repRegionName
            + " p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
        cache.getLogger().fine("Shobhit: " + resultSet);
        assertTrue(resultSet instanceof SelectResults);
        pos1 = (Position) ((SelectResults) resultSet).iterator().next();
        if (!pos1.secId.equals("APPL")) {
          fail("Query thread did not verify index results even when RE is under update");
          IndexManager.testHook = null;
        }
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server.", e);
        IndexManager.testHook = null;
      } finally {
        hooked = false;// Let client put go further.
      }
      await().until(() -> hooked);
      try {
        Object resultSet = queryService.newQuery("<trace> select pos from /" + repRegionName
            + " p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
        cache.getLogger().fine("Shobhit: " + resultSet);
        assertTrue(resultSet instanceof SelectResults);
        if (((SelectResults) resultSet).size() > 0) {
          Position pos2 = (Position) ((SelectResults) resultSet).iterator().next();
          if (pos2.equals(pos1)) {
            fail("Query thread did not verify index results even when RE is under update and "
                + "RegionEntry value has been modified before releasing the lock");
          }
        }
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server.");
      } finally {
        hooked = false;// Let client put go further.
        IndexManager.testHook = null;
      }
    });
    await().until(joinThread(putThread));
  }

  @Test
  public void testRangeIndexWithIndexAndQueryFromClauseMisMatch() {
    // Create caches
    server.invoke("create indexes", () -> {
      Cache cache = cacheRule.getCache();
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
      IndexManager.testHook = null;
      // Create common Portfolios and NewPortfolios
      Position.cnt = 0;
      for (int j = cnt; j < cntDest; j++) {
        region.put(j, new Portfolio(j));
      }

      QueryService queryService = cache.getQueryService();
      try {
        Index index = queryService.createIndex("posIndex", "pos.secId",
            "/" + repRegionName + " p, p.collectionHolderMap.values coll, p.positions.values pos");
        assertEquals(12, index.getStatistics().getNumberOfKeys());
      } catch (Exception e) {
        logger.error(e);
        fail("Index creation failed");
      }
    });
    // Invoke update from client and stop in updateIndex
    // firesultSett before updating the RegionEntry and second after updating
    // the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync("update a Region Entry", () -> {
      Region repRegion = cacheRule.getCache().getRegion(repRegionName);
      IndexManager.testHook = new IndexManagerTestHook();
      // This portfolio with same ID must have different positions.
      repRegion.put(new Integer("1"), new Portfolio(1));
      // above call must be hooked in BEFORE_UPDATE_OP call.
    });

    server.invoke("query on server", () -> {
      Cache cache = cacheRule.getCache();
      QueryService queryService = cache.getQueryService();
      Position pos1 = null;
      await().until(() -> hooked);
      try {
        Object resultSet = queryService.newQuery("<trace> select pos from /" + repRegionName
            + " p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
        cache.getLogger().fine("Shobhit: " + resultSet);
        assertTrue(resultSet instanceof SelectResults);
        pos1 = (Position) ((SelectResults) resultSet).iterator().next();
        if (!pos1.secId.equals("APPL")) {
          fail("Query thread did not verify index results even when RE is under update");
          IndexManager.testHook = null;
        }
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server.");
        IndexManager.testHook = null;
      } finally {
        hooked = false;// Let client put go further.
      }
      await().until(() -> hooked);
      try {
        Object resultSet = queryService.newQuery("select pos from /" + repRegionName
            + " p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
        assertTrue(resultSet instanceof SelectResults);
        if (((SelectResults) resultSet).size() > 0) {
          Position pos2 = (Position) ((SelectResults) resultSet).iterator().next();
          if (pos2.equals(pos1)) {
            fail("Query thread did not verify index results even when RE is under update and "
                + "RegionEntry value has been modified before releasing the lock");
          }
        }
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server.");
      } finally {
        hooked = false;// Let client put go further.
        IndexManager.testHook = null;
      }
    });
    await().until(joinThread(putThread));
  }

  @Test
  public void testRangeIndexWithIndexAndQueryFromClauseMisMatch2() {
    // Create caches
    server.invoke("create indexes", () -> {
      Cache cache = cacheRule.getCache();
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
      IndexManager.testHook = null;
      // Create common Portfolios and NewPortfolios
      Position.cnt = 0;
      for (int j = cnt; j < cntDest; j++) {
        region.put(new Integer(j), new Portfolio(j));
      }

      QueryService queryService = cache.getQueryService();
      try {
        Index index = queryService.createIndex("posIndex", "pos.secId",
            "/" + repRegionName + " p, p.positions.values pos");
        assertEquals(12, index.getStatistics().getNumberOfKeys());
      } catch (Exception e) {
        logger.error(e);
        fail("Index creation failed");
      }
    });
    // Invoke update from client and stop in updateIndex
    // firesultSett before updating the RegionEntry and second after updating
    // the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync("update a Region Entry", () -> {
      Cache cache = cacheRule.getCache();
      Region repRegion = cache.getRegion(repRegionName);
      IndexManager.testHook = new IndexManagerTestHook();
      // This portfolio with same ID must have different positions.
      repRegion.put(new Integer("1"), new Portfolio(1));
      // above call must be hooked in BEFORE_UPDATE_OP call.
    });

    server.invoke("query on server", () -> {
      Cache cache = cacheRule.getCache();
      QueryService queryService = cache.getQueryService();
      Position pos1 = null;
      await().until(() -> hooked);
      try {
        Object resultSet = queryService
            .newQuery("<trace> select pos from /" + repRegionName
                + " p, p.collectionHolderMap.values coll, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1")
            .execute();
        cache.getLogger().fine("Shobhit: " + resultSet);
        assertTrue(resultSet instanceof SelectResults);
        pos1 = (Position) ((SelectResults) resultSet).iterator().next();
        if (!pos1.secId.equals("APPL")) {
          fail("Query thread did not verify index results even when RE is under update");
          IndexManager.testHook = null;
        }
      } catch (Exception e) {
        logger.error(e);
        Assertions.fail("Query execution failed on server.", e);
        IndexManager.testHook = null;
      } finally {
        hooked = false;// Let client put go further.
      }
      await().until(() -> hooked);

      try {
        Object resultSet = queryService
            .newQuery("select pos from /" + repRegionName
                + " p, p.collectionHolderMap.values coll, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1")
            .execute();
        assertTrue(resultSet instanceof SelectResults);
        if (((SelectResults) resultSet).size() > 0) {
          Position pos2 = (Position) ((SelectResults) resultSet).iterator().next();
          if (pos2.equals(pos1)) {
            fail("Query thread did not verify index results even when RE is under update and "
                + "RegionEntry value has been modified before releasing the lock");
          }
        }
      } catch (Exception e) {
        logger.error(e);
        fail("Query execution failed on server.");
      } finally {
        IndexManager.testHook = null;
        hooked = false;// Let client put go further.
      }
    });
    await().until(joinThread(putThread));
  }

  private Callable<Boolean> joinThread(AsyncInvocation thread) {
    return () -> {
      try {
        thread.join(100L);
      } catch (InterruptedException e) {
        return false;
      }
      if (thread.isAlive()) {
        return false;
      }
      return true;
    };
  }

  private void createProxyRegs() {
    ClientCache cache = (ClientCache) cacheRule.getCache();
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(repRegionName);
  }

  public void createPR() {
    PartitionResolver testKeyBasedResolver = new QueryAPITestPartitionResolver();
    Cache cache = cacheRule.getCache();
    int numOfBuckets = 20;
    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(numOfBuckets)
            .setPartitionResolver(testKeyBasedResolver).create())
        .create(PartitionedRegionName1);
  }

  public class IndexManagerTestHook
      implements org.apache.geode.cache.query.internal.index.IndexManager.TestHook {
    public void hook(final int spot) throws RuntimeException {
      switch (spot) {
        case 9: // Before Index update and after region entry lock.
          hooked = true;
          logger
              .info("QueryDataInconsistency.IndexManagerTestHook is hooked in Update Index Entry.");
          await().until(() -> !hooked);
          break;
        case 10: // Before Region update and after Index Remove call.
          hooked = true;
          logger
              .info("QueryDataInconsistency.IndexManagerTestHook is hooked in Remove Index Entry.");
          await().until(() -> !hooked);
          break;
        default:
          break;
      }
    }
  }
}
