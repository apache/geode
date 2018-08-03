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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;


@Category({OQLIndexTest.class})
public class QueryIndexDUnitTest extends JUnit4CacheTestCase {
  public static final Logger logger = LogService.getLogger();

  public QueryIndexDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    int hostCount = Host.getHostCount();
    SerializableRunnable createRegion = new SerializableRunnable("createRegion") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("portfolios");
        if (region == null) {
          AttributesFactory attributesFactory = new AttributesFactory();
          attributesFactory.setValueConstraint(Portfolio.class);
          attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
          attributesFactory.setIndexMaintenanceSynchronous(true);
          attributesFactory.setScope(Scope.GLOBAL);
          RegionAttributes regionAttributes = attributesFactory.create();
          region = cache.createRegion("portfolios", regionAttributes);
          region.put("0", new Portfolio(0));
        }
      }
    };
    logger.info("Number of hosts = " + hostCount);
    for (int i = 0; i < hostCount; i++) {
      Host host = Host.getHost(i);
      int vmCount = host.getVMCount();
      logger.info("Host number :" + i + "contains " + vmCount + "Vms");

      for (int j = 0; j < vmCount; j++) {
        VM vm = host.getVM(j);
        vm.invoke(createRegion);
      }
    }
  }

  private static void doPut() {
    Region region = basicGetCache().getRegion("portfolios");
    if (region == null) {
      logger.info("REGION IS NULL");
    }
    try {
      region.put("1", new Portfolio(1));
      region.put("2", new Portfolio(2));
      region.put("3", new Portfolio(3));
      region.put("4", new Portfolio(4));
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to do put operation", e);
    }
  }

  private static void doDestroy() {
    Region region = basicGetCache().getRegion("portfolios");
    if (region == null) {
      logger.info("REGION IS NULL");
    }
    try {
      region.destroy("2");
      region.destroy("3");
      region.destroy("4");
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to do put operation", e);
    }
  }

  private static void doInPlaceUpdate(String str) {
    Region region = basicGetCache().getRegion("portfolios");
    Portfolio p = null;
    if (region == null) {
      logger.info("REGION IS NULL");
    }
    try {
      for (int i = 0; i <= 4; i++) {

        p = (Portfolio) region.get("" + i);
        p.status = "active";
        region.put("" + i, p);
      }
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to do put operation", e);
    }
  }

  @Test
  public void createIndexesAndUpdatesAndThenValidate() throws InterruptedException {
    Integer[] intArr = new Integer[2];
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    AsyncInvocation ai1 = null;
    AsyncInvocation ai2 = null;

    vm0.invoke(() -> QueryIndexDUnitTest.createIndex());
    vm0.invoke(() -> QueryIndexDUnitTest.validateIndexUsage());
    VM vm3 = host.getVM(3);


    vm0.invoke(() -> QueryIndexDUnitTest.removeIndex());
    ai1 = vm3.invokeAsync(() -> QueryIndexDUnitTest.doPut());
    ai2 = vm0.invokeAsync(() -> QueryIndexDUnitTest.createIndex());
    ThreadUtils.join(ai1, 30 * 1000);
    ThreadUtils.join(ai2, 30 * 1000);
    intArr[0] = new Integer(3);
    intArr[1] = new Integer(2);
    vm0.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));


    vm0.invoke(() -> QueryIndexDUnitTest.removeIndex());
    ai1 = vm0.invokeAsync(() -> QueryIndexDUnitTest.doDestroy());
    ai2 = vm0.invokeAsync(() -> QueryIndexDUnitTest.createIndex());
    ThreadUtils.join(ai1, 30 * 1000);
    ThreadUtils.join(ai2, 30 * 1000);
    intArr[0] = new Integer(1);
    intArr[1] = new Integer(1);
    vm0.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));


    vm0.invoke(() -> QueryIndexDUnitTest.removeIndex());
    ai1 = vm0.invokeAsync(() -> QueryIndexDUnitTest.doPut());
    ai2 = vm0.invokeAsync(() -> QueryIndexDUnitTest.createIndex());
    ThreadUtils.join(ai1, 30 * 1000);
    ThreadUtils.join(ai2, 30 * 1000);
    intArr[0] = new Integer(3);
    intArr[1] = new Integer(2);
    vm0.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));


    vm0.invoke(() -> QueryIndexDUnitTest.removeIndex());
    ai1 = vm3.invokeAsync(() -> QueryIndexDUnitTest.doDestroy());
    ai2 = vm0.invokeAsync(() -> QueryIndexDUnitTest.createIndex());
    ThreadUtils.join(ai1, 30 * 1000);
    ThreadUtils.join(ai2, 30 * 1000);
    intArr[0] = new Integer(1);
    intArr[1] = new Integer(1);
    vm0.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));

    // Test for in-place update.
    vm0.invoke(() -> QueryIndexDUnitTest.removeIndex());
    ai1 = vm0.invokeAsync(() -> QueryIndexDUnitTest.doPut());
    ai2 = vm0.invokeAsync(() -> QueryIndexDUnitTest.createIndex());
    ThreadUtils.join(ai1, 30 * 1000);
    ThreadUtils.join(ai2, 30 * 1000);
    intArr[0] = new Integer(3);
    intArr[1] = new Integer(2);
    vm0.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));

    try {
      Thread.sleep(2000);
    } catch (Exception ex) {
    }
    // Do an in-place update of the region entries.
    // This will set the Portfolio objects status to "active".
    String str = "To get Update in synch thread."; // Else test was exiting before the validation
                                                   // could finish.
    vm0.invoke(() -> doInPlaceUpdate(str));
    intArr[0] = new Integer(5);
    intArr[1] = new Integer(0);
    vm0.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));
  }

  @Test
  public void createIndexOnOverflowRegionsAndExecuteQueries() throws InterruptedException {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] {host.getVM(0), host.getVM(1), host.getVM(2), host.getVM(3),};

    // Create and load regions on all vms.
    for (int i = 0; i < vms.length; i++) {
      int finalI = i;
      vms[i].invoke(() -> QueryIndexDUnitTest.createAndLoadOverFlowRegions("testOf" + "vm" + finalI,
          new Boolean(true), new Boolean(true)));
    }

    // Create index on the regions.
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(() -> QueryIndexDUnitTest.createIndexOnOverFlowRegions());
    }

    // execute query.
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(() -> QueryIndexDUnitTest.executeQueriesUsingIndexOnOverflowRegions());
    }

    // reload the regions after index creation.
    for (int i = 0; i < vms.length; i++) {
      int finalI = i;
      vms[i].invoke(() -> QueryIndexDUnitTest.createAndLoadOverFlowRegions("testOf" + "vm" + finalI,
          new Boolean(false), new Boolean(true)));
    }

    // reexecute the query.
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(() -> QueryIndexDUnitTest.executeQueriesUsingIndexOnOverflowRegions());
    }
  }

  @Test
  public void createIndexOnOverflowRegionsAndValidateResults() throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] {host.getVM(0), host.getVM(1),};

    // Create and load regions on all vms.
    for (int i = 0; i < vms.length; i++) {
      int finalI = i;
      vms[i].invoke(() -> QueryIndexDUnitTest.createAndLoadOverFlowRegions(
          "testOfValid" + "vm" + finalI, new Boolean(true), new Boolean(false)));
    }

    vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = basicGetCache();
        String[] regionNames = new String[] {"replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};

        QueryService qs = cache.getQueryService();
        Region region = null;

        int numObjects = 10;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 1; cnt < numObjects; cnt++) {
            region.put(new Portfolio(cnt), new Portfolio(cnt));
          }
        }

        String[] qString = new String[] {"SELECT * FROM /REGION_NAME pf WHERE pf.ID = 1",
            "SELECT ID FROM /REGION_NAME pf WHERE pf.ID = 1",
            "SELECT * FROM /REGION_NAME pf WHERE pf.ID > 5",
            "SELECT ID FROM /REGION_NAME pf WHERE pf.ID > 5",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID = 1",
            "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 5",
            "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID > 5",};

        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];

        int r = 0;
        try {
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] = (SelectResults) query.execute();
              r++;
            }
          }
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        try {
          for (int i = 0; i < regionNames.length; i++) {
            region = cache.getRegion(regionNames[i]);
            String indexName = "idIndex" + regionNames[i];
            cache.getLogger()
                .fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i1 = qs.createIndex(indexName, "pf.ID", "/" + regionNames[i] + " pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i2 = qs.createIndex(indexName, "key.ID", "/" + regionNames[i] + ".keys key");
              }
            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to create index", ex);
          fail("Failed to create index.");
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults) query.execute();
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < srWithIndex.length; i++) {
          sr[0][0] = srWithoutIndex[i];
          sr[0][1] = srWithIndex[i];
          logger.info("Comparing the result for the query : " + queryString[i]
              + " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, queryString);
        }

        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 1; cnt < numObjects; cnt++) {
            if (cnt % 2 == 0) {
              region.destroy(new Portfolio(cnt));
            }
          }
          for (int cnt = 10; cnt < numObjects; cnt++) {
            if (cnt % 2 == 0) {
              region.put(new Portfolio(cnt), new Portfolio(cnt));
            }
          }
        }

        // Execute Query with index.
        srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults) query.execute();
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

      }
    });
  }

  @Test
  public void createIndexOnOverflowRegionsWithMultipleWhereClauseAndValidateResults()
      throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] {host.getVM(0), host.getVM(1),};

    // Create and load regions on all vms.
    for (int i = 0; i < vms.length; i++) {
      int finalI = i;
      vms[i].invoke(() -> QueryIndexDUnitTest.createAndLoadOverFlowRegions(
          "testOfValid2" + "vm" + finalI, new Boolean(true), new Boolean(false)));
    }

    vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = basicGetCache();
        String[] regionNames = new String[] {"replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};

        QueryService qs = cache.getQueryService();
        Region region = null;

        int numObjects = 10;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 1; cnt < numObjects; cnt++) {
            region.put(new Portfolio(cnt), new String("XX" + cnt));
          }
        }

        String[] qString = new String[] {"SELECT * FROM /REGION_NAME pf WHERE pf = 'XX1'",
            "SELECT * FROM /REGION_NAME pf WHERE pf IN SET( 'XX5', 'XX6', 'XX7')",
            "SELECT * FROM /REGION_NAME.values pf WHERE pf IN SET( 'XX5', 'XX6', 'XX7')",
            "SELECT * FROM /REGION_NAME.keys k WHERE k.ID = 1",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
            "SELECT ID, status FROM /REGION_NAME.keys WHERE ID = 1",
            "SELECT k.ID, k.status FROM /REGION_NAME.keys k WHERE k.ID = 1 and k.status = 'active'",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 5",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID > 5 and key.status = 'active'",};

        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];

        int r = 0;
        try {
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] = (SelectResults) query.execute();
              r++;
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        String indexName = "";
        try {
          for (int i = 0; i < regionNames.length; i++) {
            region = cache.getRegion(regionNames[i]);
            indexName = "idIndex" + regionNames[i];
            cache.getLogger()
                .fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i1 = qs.createIndex(indexName, "pf", "/" + regionNames[i] + " pf");
              }
              indexName = "valueIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i1 = qs.createIndex(indexName, "pf", "/" + regionNames[i] + ".values pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i2 = qs.createIndex(indexName, "key.ID", "/" + regionNames[i] + ".keys key");
              }
              indexName = "keyIdIndex2" + regionNames[i];


            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to create index", ex);
          fail("Failed to create index." + indexName);
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults) query.execute();
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < srWithIndex.length; i++) {
          sr[0][0] = srWithoutIndex[i];
          sr[0][1] = srWithIndex[i];
          logger.info("Comparing the result for the query : " + queryString[i]
              + " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, queryString);
        }

        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 1; cnt < numObjects; cnt++) {
            if (cnt % 2 == 0) {
              region.destroy(new Portfolio(cnt));
            }
          }
          for (int cnt = 10; cnt < numObjects; cnt++) {
            if (cnt % 2 == 0) {
              region.put(new Portfolio(cnt), new String("XX" + cnt));
            }
          }
        }

        // Execute Query with index.
        srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults) query.execute();
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

      }
    });
  }

  @Test
  public void createIndexAndUpdatesOnOverflowRegionsAndValidateResults() throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] {host.getVM(0), host.getVM(1),};

    // Create and load regions on all vms.
    for (int i = 0; i < vms.length; i++) {
      int finalI = i;
      vms[i].invoke(() -> QueryIndexDUnitTest.createAndLoadOverFlowRegions(
          "testOfValid3" + "vm" + finalI, new Boolean(true), new Boolean(true)));
    }

    vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = basicGetCache();
        String[] regionNames = new String[] {"replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};

        QueryService qs = cache.getQueryService();
        Region region = null;


        String[] qString = new String[] {"SELECT * FROM /REGION_NAME pf WHERE pf.ID = 1",
            "SELECT ID FROM /REGION_NAME pf WHERE pf.ID = 1",
            "SELECT * FROM /REGION_NAME pf WHERE pf.ID > 5",
            "SELECT ID FROM /REGION_NAME pf WHERE pf.ID > 5",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID = 1",
            "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 5",
            "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID > 5",};

        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];

        int r = 0;
        try {
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] = (SelectResults) query.execute();
              r++;
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        try {
          for (int i = 0; i < regionNames.length; i++) {
            region = cache.getRegion(regionNames[i]);
            String indexName = "idIndex" + regionNames[i];
            cache.getLogger()
                .fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i1 = qs.createIndex(indexName, "pf.ID", "/" + regionNames[i] + " pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i2 = qs.createIndex(indexName, "key.ID", "/" + regionNames[i] + ".keys key");
              }
            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to create index", ex);
          fail("Failed to create index.");
        }

        int numObjects = 50;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.

        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 0; cnt < numObjects; cnt++) {
            region.put(new Portfolio(cnt), new Portfolio(cnt));
          }
        }

        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          String indexName = "idIndex" + regionNames[i];
          Index i1, i2;
          if ((i1 = qs.getIndex(region, indexName)) != null) {
            assertEquals("Unexpected number of keys in the index ", numObjects,
                i1.getStatistics().getNumberOfKeys());
            assertEquals("Unexpected number of values in the index ", numObjects,
                i1.getStatistics().getNumberOfValues());
          }
          indexName = "keyIdIndex" + regionNames[i];
          if ((i2 = qs.getIndex(region, indexName)) != null) {
            assertEquals("Unexpected number of keys in the index ", numObjects,
                i2.getStatistics().getNumberOfKeys());
            assertEquals("Unexpected number of values in the index ", numObjects,
                i2.getStatistics().getNumberOfValues());
          }
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults) query.execute();
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < srWithIndex.length; i++) {
          sr[0][0] = srWithoutIndex[i];
          sr[0][1] = srWithIndex[i];
          logger.info("Comparing the result for the query : " + queryString[i]
              + " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, queryString);
        }

      }
    });
  }

  @Test
  public void createIndexOnOverflowRegionsAndValidateResultsUsingParams() throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] {host.getVM(0), host.getVM(1),};

    // Create and load regions on all vms.
    for (int i = 0; i < vms.length; i++) {
      int finalI = i;
      vms[i].invoke(() -> QueryIndexDUnitTest.createAndLoadOverFlowRegions(
          "testOfValidUseParams" + "vm" + finalI, new Boolean(true), new Boolean(false)));
    }

    vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = basicGetCache();
        String[] regionNames = new String[] {"replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};

        QueryService qs = cache.getQueryService();
        Region region = null;

        int numObjects = 10;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 1; cnt < numObjects; cnt++) {
            region.put(new Portfolio(cnt), new Integer(cnt + 100));
          }
        }

        String[] qString = new String[] {"SELECT * FROM /REGION_NAME pf WHERE pf = $1",
            "SELECT * FROM /REGION_NAME pf WHERE pf > $1",
            "SELECT * FROM /REGION_NAME.values pf WHERE pf < $1",
            "SELECT * FROM /REGION_NAME.keys k WHERE k.ID = $1",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID = $1",
            "SELECT ID, status FROM /REGION_NAME.keys WHERE ID = $1",
            "SELECT k.ID, k.status FROM /REGION_NAME.keys k WHERE k.ID = $1 and k.status = $2",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > $1",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID > $1 and key.status = $2",};

        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];

        int r = 0;
        try {
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] =
                  (SelectResults) query.execute(new Object[] {new Integer(5), "active"});
              r++;
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        String indexName = "";
        try {
          for (int i = 0; i < regionNames.length; i++) {
            region = cache.getRegion(regionNames[i]);
            indexName = "idIndex" + regionNames[i];
            cache.getLogger()
                .fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i1 = qs.createIndex(indexName, "pf", "/" + regionNames[i] + " pf");
              }
              indexName = "valueIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i1 = qs.createIndex(indexName, "pf", "/" + regionNames[i] + ".values pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger()
                    .fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: "
                        + indexName);
                Index i2 = qs.createIndex(indexName, "key.ID", "/" + regionNames[i] + ".keys key");
              }
            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to create index", ex);
          fail("Failed to create index." + indexName);
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] =
                  (SelectResults) query.execute(new Object[] {new Integer(5), "active"});
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < srWithIndex.length; i++) {
          sr[0][0] = srWithoutIndex[i];
          sr[0][1] = srWithIndex[i];
          logger.info("Comparing the result for the query : " + queryString[i]
              + " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, queryString);
        }

        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i = 0; i < regionNames.length; i++) {
          region = cache.getRegion(regionNames[i]);
          for (int cnt = 1; cnt < numObjects; cnt++) {
            if (cnt % 2 == 0) {
              region.destroy(new Portfolio(cnt));
            }
          }
          // Add destroyed entries
          for (int cnt = 1; cnt < numObjects; cnt++) {
            if (cnt % 2 == 0) {
              region.put(new Portfolio(cnt), new Integer(cnt + 100));
            }
          }
        }

        // Execute Query with index.
        srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;
          for (int q = 0; q < qString.length; q++) {
            for (int i = 0; i < regionNames.length; i++) {
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] =
                  (SelectResults) query.execute(new Object[] {new Integer(5), "active"});
              if (!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        for (int i = 0; i < srWithIndex.length; i++) {
          sr[0][0] = srWithoutIndex[i];
          sr[0][1] = srWithIndex[i];
          logger.info("Comparing the result for the query : " + queryString[i]
              + " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, queryString);
        }
      }
    });
  }

  @Test
  public void validateIndexesAfterInPlaceUpdates() throws CacheException {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);
    final VM server2 = host.getVM(2);
    Integer[] intArr = new Integer[2];

    // create indexes
    server1.invoke(() -> QueryIndexDUnitTest.createIndex());
    server2.invoke(() -> QueryIndexDUnitTest.createIndex());

    // puts on server2
    server2.invoke(() -> QueryIndexDUnitTest.doPut());
    // Do an in-place update of the region entries on server1
    // This will set the Portfolio objects status to "active".
    String str = "To get Update in synch thread."; // Else test was exiting before the validation
                                                   // could finish.
    server1.invoke(() -> doInPlaceUpdate(str));
    intArr[0] = new Integer(5);
    intArr[1] = new Integer(0);
    server1.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));
  }

  @Test
  public void testInPlaceUpdatesWithNulls() throws CacheException {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);
    final VM server2 = host.getVM(2);

    helpTestDoInplaceUpdates(server1, server2);
    server1.invoke(() -> QueryIndexDUnitTest.setInPlaceUpdate());
    server2.invoke(() -> QueryIndexDUnitTest.setInPlaceUpdate());
    helpTestDoInplaceUpdates(server1, server2);
  }

  private void helpTestDoInplaceUpdates(VM server1, VM server2) {
    // create indexes
    server1.invoke(() -> QueryIndexDUnitTest.createIndex());
    server2.invoke(() -> QueryIndexDUnitTest.createIndex());

    Integer[] intArr = new Integer[2];
    // puts on server2
    server2.invoke(() -> QueryIndexDUnitTest.doPutNulls());
    // Do an in-place update of the region entries on server1
    // This will set the Portfolio objects status to "active".
    String str = "To get Update in synch thread."; // Else test was exiting before the validation
                                                   // could finish.
    server1.invoke(() -> doInPlaceUpdate(str));
    intArr[0] = new Integer(5);
    intArr[1] = new Integer(0);
    server1.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));

    server2.invoke(() -> QueryIndexDUnitTest.doPutNulls());

    intArr[0] = new Integer(0);
    intArr[1] = new Integer(0);
    server1.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));

    // This will set the Portfolio objects status to null.
    server1.invoke(() -> doInPlaceUpdateWithNulls(str));
    intArr[0] = new Integer(0);
    intArr[1] = new Integer(0);
    server1.invoke(() -> validateIndexUpdate(intArr[0], intArr[1]));

    // remove indexes
    server1.invoke(() -> QueryIndexDUnitTest.removeIndex());
    server2.invoke(() -> QueryIndexDUnitTest.removeIndex());

  }

  private static void doInPlaceUpdateWithNulls(String str) {
    Region region = basicGetCache().getRegion("portfolios");
    Portfolio p = null;
    if (region == null) {
      logger.info("REGION IS NULL");
    }
    try {
      for (int i = 0; i <= 4; i++) {

        p = (Portfolio) region.get("" + i);
        p.status = null;
        region.put("" + i, p);
      }
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to do put operation", e);
    }
  }

  private static void setInPlaceUpdate() {
    IndexManager.INPLACE_OBJECT_MODIFICATION_FOR_TEST = true;
  }

  private static void doPutNulls() {
    Region region = basicGetCache().getRegion("portfolios");
    if (region == null) {
      logger.info("REGION IS NULL");
    }
    try {
      for (int i = 0; i <= 4; i++) {
        Portfolio p = new Portfolio(i);
        p.status = null;
        region.put("" + i, p);
      }
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to do put operation", e);
    }
  }

  private static void executeQueriesUsingIndexOnOverflowRegions() throws Exception {
    Cache cache = basicGetCache();
    String[] regionNames = new String[] {"replicateOverFlowRegion",
        "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};


    QueryService qs = cache.getQueryService();
    Region region = null;

    String[] qString = new String[] {"SELECT * FROM /REGION_NAME pf WHERE pf.ID = 1",
        "SELECT ID FROM /REGION_NAME pf WHERE pf.ID = 1",
        "SELECT * FROM /REGION_NAME pf WHERE pf.ID > 10",
        "SELECT ID FROM /REGION_NAME pf WHERE pf.ID > 10",
        "SELECT * FROM /REGION_NAME.keys key WHERE key.ID = 1",
        "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
        "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 10",
        "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID > 10",
        "SELECT entry.value FROM /REGION_NAME.entries entry WHERE entry.value.ID = 1",
        "SELECT entry.key FROM /REGION_NAME.entries entry WHERE entry.value.ID > 10",
        "SELECT entry.getValue() FROM /REGION_NAME.entries entry WHERE entry.getValue().getID() = 1",
        "SELECT entry.getKey() FROM /REGION_NAME.entries entry WHERE entry.getValue().getID() > 10",
        "SELECT entry.getValue() FROM /REGION_NAME.entries entry WHERE entry.getValue().boolFunction('active') = false",};

    for (int q = 0; q < qString.length; q++) {
      for (int i = 0; i < regionNames.length; i++) {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
        Query query = qs.newQuery(queryStr);
        SelectResults results = (SelectResults) query.execute();
        logger.info("Executed query :" + queryStr + " Result size: " + results.asList().size());
        if (!observer.isIndexesUsed) {
          fail("Index not used for query. " + queryStr);
        }
      }
    }
  }

  private static void createIndexOnOverFlowRegions() throws Exception {
    Cache cache = basicGetCache();
    String[] regionNames = new String[] {"replicateOverFlowRegion",
        "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};

    cache.getLogger().fine("createIndexOnOverFlowRegions()");

    QueryService qs = cache.getQueryService();
    Region region = null;
    for (int i = 0; i < regionNames.length; i++) {
      region = cache.getRegion(regionNames[i]);
      String indexName = "idIndex" + regionNames[i];
      cache.getLogger().fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
      try {
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine(
              "createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf.ID",
              "/" + regionNames[i] + " pf");
        }
        indexName = "keyIdIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine(
              "createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "key.ID",
              "/" + regionNames[i] + ".keys key");
        }
        indexName = "entryIdIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine(
              "createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "entry.value.ID",
              "/" + regionNames[i] + ".entries entry");
        }
        indexName = "entryMethodIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine(
              "createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "entry.getValue().getID()",
              "/" + regionNames[i] + ".entries entry");
        }
        indexName = "entryMethodWithArgIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine(
              "createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL,
              "entry.getValue().boolFunction('active')", "/" + regionNames[i] + ".entries entry");
        }
      } catch (IndexNameConflictException ice) {
        // Ignore. The pr may have created the index through
        // remote index create message from peer.
      }
    }
  }

  private static void createAndLoadOverFlowRegions(String vmName, final Boolean createRegions,
      final Boolean loadRegions) throws Exception {
    Cache cache = basicGetCache();
    String[] regionNames = new String[] {"replicateOverFlowRegion",
        "replicatePersistentOverFlowRegion", "prOverFlowRegion", "prPersistentOverFlowRegion",};

    logger.info("CreateAndLoadOverFlowRegions() with vmName " + vmName + " createRegions: "
        + createRegions + " And LoadRegions: " + loadRegions);

    if (createRegions.booleanValue()) {
      for (int i = 0; i < regionNames.length; i++) {
        logger.info("Started creating region :" + regionNames[i]);
        String diskStore = regionNames[i] + vmName + "DiskStore";
        logger.info("Setting disk store to: " + diskStore);
        cache.createDiskStoreFactory().create(diskStore);
        try {
          AttributesFactory attributesFactory = new AttributesFactory();
          attributesFactory.setDiskStoreName(diskStore);
          attributesFactory.setEvictionAttributes(
              EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.OVERFLOW_TO_DISK));
          if (i == 0) {
            attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
          } else if (i == 1) {
            attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          } else if (i == 2) {
            attributesFactory.setDataPolicy(DataPolicy.PARTITION);
          } else {
            attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          }
          cache.createRegion(regionNames[i], attributesFactory.create());
          logger.info("Completed creating region :" + regionNames[i]);
        } catch (Exception e) {
          Assert.fail("Could not create region" + regionNames[i], e);
        }
      }
    }
    Region region = null;
    int numObjects = 50;
    if (loadRegions.booleanValue()) {
      for (int i = 0; i < regionNames.length; i++) {
        region = cache.getRegion(regionNames[i]);
        // If its just load, try destroy some entries and reload.
        if (!createRegions.booleanValue()) {
          logger.info("Started destroying region entries:" + regionNames[i]);
          for (int cnt = 0; cnt < numObjects / 3; cnt++) {
            region.destroy(new Portfolio(cnt * 2));
          }
        }

        logger.info("Started Loading region :" + regionNames[i]);
        for (int cnt = 0; cnt < numObjects; cnt++) {
          region.put(new Portfolio(cnt), new Portfolio(cnt));
        }
        logger.info("Completed loading region :" + regionNames[i]);
      }
    }
  }

  private static void validateIndexUpdate(Integer a, Integer b) {
    QueryService qs = basicGetCache().getQueryService();
    Query q = qs.newQuery("SELECT DISTINCT * FROM /portfolios where status = 'active'");
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    Object r;
    try {
      r = q.execute();
      int actual = ((Collection) r).size();
      if (actual != a.intValue()) {
        fail("Active NOT of the expected size, found " + actual + ", expected " + a.intValue());
      }
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to query", e);
    }
    if (!observer.isIndexesUsed) {
      fail("Index not used for query");
    }

    q = qs.newQuery("SELECT DISTINCT * FROM /portfolios where status = 'inactive'");
    observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);

    try {
      r = q.execute();
      int actual = ((Collection) r).size();
      if (actual != b.intValue()) {
        fail("Inactive NOT of the expected size, found " + actual + ", expected " + b.intValue());
      }
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to query", e);
    }
    if (!observer.isIndexesUsed) {
      fail("Index not used for query");
    }
  }

  private static void createIndex() {
    Region region = basicGetCache().getRegion("portfolios");
    if (region == null) {
      logger.info("The region is not created properly");
    } else {
      if (!region.isDestroyed()) {
        logger.info("Obtained the region with name :" + "portfolios");
        QueryService qs = basicGetCache().getQueryService();
        if (qs != null) {
          try {
            qs.createIndex("statusIndex", "status", "/portfolios");
            logger.info("Index statusIndex Created successfully");
          } catch (IndexNameConflictException e) {
            Assert.fail("Caught IndexNameConflictException", e);
          } catch (IndexExistsException e) {
            Assert.fail("Caught IndexExistsException", e);
          } catch (QueryException e) {
            Assert.fail("Caught exception while trying to create index", e);
          }
        } else {
          fail("Could not obtain QueryService for the cache ");
        }
      } else {
        fail("Region.isDestroyed() returned true for region : " + "/portfolios");
      }
    }
  }

  private static void validateIndexUsage() {
    QueryService qs = basicGetCache().getQueryService();
    Query q = qs.newQuery("SELECT DISTINCT * FROM /portfolios where status = 'active'");
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    Object r;
    try {
      r = q.execute();
      int actual = ((Collection) r).size();
      if (actual != 1) {
        fail("Active NOT of the expected size, found " + actual + ", expected 1");
      }
    } catch (Exception e) {
      Assert.fail("Caught exception while trying to query", e);
    }
    if (!observer.isIndexesUsed) {
      fail("Index not used for query");
    }
  }

  private static void removeIndex() {
    Region region = basicGetCache().getRegion("portfolios");
    if (region == null) {
      fail("The region is not created properly");
    } else {
      logger.info("Obtained the region with name :" + "portfolios");
      QueryService qs = basicGetCache().getQueryService();
      if (qs != null) {
        try {
          Collection indexes = qs.getIndexes(region);
          if (indexes.isEmpty()) {
            return; // no IndexManager defined
          }

          Iterator iter = indexes.iterator();
          if (iter.hasNext()) {
            Index idx = (Index) (iter.next());
            String name = idx.getName();
            qs.removeIndex(idx);
            logger.info("Index " + name + " removed successfully");
          }
        } catch (Exception e) {
          Assert.fail("Caught exception while trying to remove index", e);
        }
      } else {
        fail("Unable to obtain QueryService for the cache ");
      }

    }
  }

  public static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }

}
