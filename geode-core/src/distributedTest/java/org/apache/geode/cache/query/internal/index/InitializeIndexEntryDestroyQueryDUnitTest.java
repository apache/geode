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

import static org.apache.geode.cache.query.Utils.createPortfolioData;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.cache.query.partitioned.PRQueryDUnitHelper;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * Test creates a local region. Creates and removes index in a parallel running thread. Then
 * destroys and puts back entries in separated thread in the same region and runs query in parallel
 * and checks for UNDEFINED values in result set of the query.
 */
@Category({OQLIndexTest.class})
@RunWith(JUnitParamsRunner.class)
public class InitializeIndexEntryDestroyQueryDUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  final Portfolio portfolio = new Portfolio(1, 1);

  private int cnt = 0;

  private int cntDest = 100;

  public InitializeIndexEntryDestroyQueryDUnitTest() {
    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  private static Scope[] getScope() {
    return new Scope[] {Scope.LOCAL, null};
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.query.data.**");
    return properties;
  }


  @Test
  @Parameters(method = "getScope")
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnPR(Scope scope) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    String regionName = "PartionedPortfoliosPR";
    String query = "select * from /" + regionName + " p where p.status = 'active'";
    try {
      vm0.invoke(() -> createRegionInVM(regionName, scope));
      final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
      vm0.invoke(
          PRQHelp.getCacheSerializableRunnableForPRPuts(regionName, portfolio, cnt, cntDest));
      AsyncInvocation asyInvk0 =
          vm0.invokeAsync(() -> consecutivelyCreateAndDestroyIndex(regionName));
      AsyncInvocation asyInvk1 =
          vm0.invokeAsync(() -> consecutivelyPutAndDestroyEntries(regionName));
      vm0.invoke(() -> executeAndValidateQueryResults(query));
      waitForAsyncThreadsToComplete(asyInvk0, asyInvk1);
    } finally {
      vm0.invoke(() -> clearIndexesAndDestroyRegion(regionName));
    }
  }

  @Test
  public void testConcurrentRemoveIndexAndQueryOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    String name = "PartionedPortfoliosPR";
    String query =
        "select * from /" + name + " p where p.status = 'active' and p.ID > 0 and p.pk != ' ' ";
    try {
      vm0.invoke(() -> createRegionInVM(name, null));
      final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
      vm0.invoke(() -> createIndex(name, "statusIndex", "p.status", "/" + name + " p"));
      vm0.invoke(() -> createIndex(name, "idIndex", "p.ID", "/" + name + " p"));
      vm0.invoke(() -> createIndex(name, "pkidIndex", "p.pk", "/" + name + " p"));
      vm0.invoke(() -> executeAndValidateQueryResults(query));
    } finally {
      vm0.invoke(() -> clearIndexesAndDestroyRegion(name));
    }

  }

  private void waitForAsyncThreadsToComplete(AsyncInvocation asyInvk0, AsyncInvocation asyInvk1) {
    ThreadUtils.join(asyInvk0, 1000 * 1000); // TODO: this is way too long: 16.67 minutes!
    if (asyInvk0.exceptionOccurred()) {
      logger.error("Asynchronous thread to create and destroy index failed",
          asyInvk0.getException());
      fail();
    }

    ThreadUtils.join(asyInvk1, 1000 * 1000); // TODO: this is way too long: 16.67 minutes!
    if (asyInvk1.exceptionOccurred()) {
      logger.error("Asychronous thread to create and destroy region entry failed with exception",
          asyInvk1.getException());
      fail();
    }
  }

  private void createRegionInVM(String regionName, Scope scope) {
    Cache cache = getCache();
    Region partitionRegion = null;
    try {
      RegionFactory regionFactory = cache.createRegionFactory();
      if (scope != null) {
        partitionRegion = regionFactory.setValueConstraint(PortfolioData.class)
            .setIndexMaintenanceSynchronous(false).setScope(scope).create(regionName);
      } else {
        partitionRegion = regionFactory.setValueConstraint(PortfolioData.class)
            .setIndexMaintenanceSynchronous(false).setDataPolicy(DataPolicy.PARTITION)
            .create(regionName);
      }
    } catch (IllegalStateException ex) {
      logger.warn("Creation caught IllegalStateException", ex);
    }
    assertNotNull("Region " + regionName + " not in cache", cache.getRegion(regionName));
    assertNotNull("Region ref null", partitionRegion);
    assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
  }

  private void consecutivelyCreateAndDestroyIndex(String regionName) {
    for (int i = 0; i < cntDest; i++) {
      // Create Index first to go in hook.
      Cache cache = getCache();
      Index index = null;
      try {
        index =
            cache.getQueryService().createIndex("statusIndex", "p.status", "/" + regionName + " p");
      } catch (Exception e1) {
        logger.error("Index creation failed", e1);
        fail();
      }
      Region region = cache.getRegion(regionName);
      await()
          .untilAsserted(
              () -> assertNotNull(cache.getQueryService().getIndex(region, "statusIndex")));
      getCache().getQueryService().removeIndex(index);
      await().untilAsserted(
          () -> assertEquals(null, getCache().getQueryService().getIndex(region, "statusIndex")));
    }
  }

  private void consecutivelyPutAndDestroyEntries(String regionName) {
    Region r = getCache().getRegion(regionName);

    for (int i = 0, j = 0; i < 500; i++, j++) {

      PortfolioData p = (PortfolioData) r.get(j);
      logger.debug("Going to destroy the value" + p);
      r.destroy(j);
      final int key = j;
      await()
          .untilAsserted(() -> assertEquals(null, r.get(key)));

      // Put the value back again.
      getCache().getLogger().fine("Putting the value back" + p);
      r.put(j, p);

      // Reset j
      if (j == cntDest - 1) {
        j = 0;
      }
    }
  }

  private void executeAndValidateQueryResults(String queryString) {
    Query query = getCache().getQueryService().newQuery(queryString);
    // Now run the query
    SelectResults results = null;
    for (int i = 0; i < 50; i++) {
      try {
        logger.debug("Querying the region");
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Query Execution failed", e);
        fail();
      }
      for (Object obj : results) {
        if (obj instanceof Undefined) {
          fail("Found an undefined element" + Arrays.toString(results.toArray()));
        }
      }
    }
  }

  private void clearIndexesAndDestroyRegion(String regionName) {
    Region region = getCache().getRegion(regionName);
    if (region != null) {
      getCache().getQueryService().removeIndexes(region);
      region.destroyRegion();
    }
  }

  private void createIndex(String regionName, String indexName, String expression,
      String regionPath) {
    try {
      getCache().getQueryService().createIndex(indexName, expression, regionPath);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Exception while creating index : " + indexName, e);
      fail();
    }
    Region region = getCache().getRegion(regionName);
    await()
        .untilAsserted(
            () -> assertNotNull(getCache().getQueryService().getIndex(region, indexName)));
  }
}
