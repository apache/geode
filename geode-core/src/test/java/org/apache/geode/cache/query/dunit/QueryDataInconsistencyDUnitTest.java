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
package org.apache.geode.cache.query.dunit;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.partitioned.PRQueryDUnitHelper;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.internal.cache.execute.PRClientServerTestBase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 * This tests the data inconsistency during update on an index and querying the
 * same UNLOCKED index.
 */
@Category(DistributedTest.class)
public class QueryDataInconsistencyDUnitTest extends JUnit4CacheTestCase {

  private static final int cnt = 0;

  private static final int cntDest = 10;

  static VM server = null;

  static VM client = null;

  static Cache cache = null;

  static String PartitionedRegionName1 = "TestPartitionedRegion1"; // default
                                                                   // name
  static String repRegionName = "TestRepRegion"; // default name
  
  static Integer serverPort1 = null;

  public static int numOfBuckets = 20;

  public static String[] queries = new String[] {
      "select * from /" + PartitionedRegionName1 + " where ID=1",
      };

  public static String[] queriesForRR = new String[] { "<trace> select * from /"
      + repRegionName + " where ID=1" };

  public static volatile boolean hooked = false;
  /**
   * @param name
   */
  public QueryDataInconsistencyDUnitTest() {
    super();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> disconnectFromDS());
    Invoke.invokeInEveryVM(() -> QueryObserverHolder.reset());
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    server = host.getVM(0);
  }

  @Test
  public void testCompactRangeIndex() {
    // Create caches
    Properties props = new Properties();
    server.invoke(() -> PRClientServerTestBase.createCacheInVm( props ));

    server.invoke(new CacheSerializableRunnable("create indexes") {

      @Override
      public void run2() throws CacheException {
        cache = CacheFactory.getAnyInstance();
        Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
        
        // Create common Portflios and NewPortfolios
        for (int j = cnt; j < cntDest; j++) {
          region.put(new Integer(j), new Portfolio(j));
        }
        
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        try {
          Index index = qs.createIndex("idIndex", "ID", "/"+repRegionName);
          assertEquals(10, index.getStatistics().getNumberOfKeys());
        } catch (Exception e) {
          fail("Index creation failed");
        }
      }
    });
    //Invoke update from client and stop in updateIndex
    //first before updating the RegionEntry and second after updating
    //the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync(new CacheSerializableRunnable("update a Region Entry") {
      
      @Override
      public void run2() throws CacheException {
        Region repRegion = CacheFactory.getAnyInstance().getRegion(repRegionName);
        IndexManager.testHook = new IndexManagerTestHook();
        repRegion.put(new Integer("1"), new Portfolio(cntDest+1));
        //above call must be hooked in BEFORE_UPDATE_OP call.
      }
    });
    server.invoke(new CacheSerializableRunnable("query on server") {
      
      @Override
      public void run2() throws CacheException {
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        while (!hooked){Wait.pause(100);}
        Object rs = null;
        try {
          rs = qs.newQuery("<trace> select * from /"+repRegionName+" where ID = 1").execute();          
        } catch (Exception e) {
          e.printStackTrace();
          fail("Query execution failed on server.");
          IndexManager.testHook = null;
        }
        assertTrue(rs instanceof SelectResults);
        assertEquals(1, ((SelectResults)rs).size());
        Portfolio p1 = (Portfolio) ((SelectResults)rs).asList().get(0);
        if (p1.getID() != 1) {
          fail("Query thread did not verify index results even when RE is under update");
          IndexManager.testHook = null;
        }
        hooked = false;//Let client put go further.
      }
    });

    //Client put is again hooked in AFTER_UPDATE_OP call in updateIndex.
    server.invoke(new CacheSerializableRunnable("query on server") {
      
      @Override
      public void run2() throws CacheException {
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        while (!hooked){Wait.pause(100);}
        Object rs = null;
        try {
          rs = qs.newQuery("<trace> select * from /"+repRegionName+" where ID = 1").execute();          
        } catch (Exception e) {
          e.printStackTrace();
          fail("Query execution failed on server."+e.getMessage());
        } finally {
          IndexManager.testHook = null;
        }
        assertTrue(rs instanceof SelectResults);
        if (((SelectResults)rs).size() > 0) {
          Portfolio p1 = (Portfolio) ((SelectResults)rs).iterator().next();
          if (p1.getID() != 1) {
            fail("Query thread did not verify index results even when RE is under update and " +
                      "RegionEntry value has been modified before releasing the lock");
            IndexManager.testHook = null;
          }
        }
        hooked = false;//Let client put go further.
      }
    });
    ThreadUtils.join(putThread, 200);
  }

  @Test
  public void testRangeIndex() {
    // Create caches
    Properties props = new Properties();
    server.invoke(() -> PRClientServerTestBase.createCacheInVm( props ));

    server.invoke(new CacheSerializableRunnable("create indexes") {

      @Override
      public void run2() throws CacheException {
        cache = CacheFactory.getAnyInstance();
        Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
        IndexManager.testHook = null;
        // Create common Portfolios and NewPortfolios
        Position.cnt = 0;
        for (int j = cnt; j < cntDest; j++) {
          Portfolio p = new Portfolio(j);
          CacheFactory.getAnyInstance().getLogger().fine("Shobhit: portfolio "+j+ " : "+p);
          region.put(new Integer(j), p);
        }
        
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        try {
          Index index = qs.createIndex("posIndex", "pos.secId", "/"+repRegionName+" p, p.positions.values pos");
          assertEquals(12, index.getStatistics().getNumberOfKeys());
        } catch (Exception e) {
          fail("Index creation failed");
        }
      }
    });
    //Invoke update from client and stop in updateIndex
    //first before updating the RegionEntry and second after updating
    //the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync(new CacheSerializableRunnable("update a Region Entry") {
      
      @Override
      public void run2() throws CacheException {
        Region repRegion = CacheFactory.getAnyInstance().getRegion(repRegionName);
        IndexManager.testHook = new IndexManagerTestHook();
        Portfolio newPort = new Portfolio(cntDest+1);
        CacheFactory.getAnyInstance().getLogger().fine("Shobhit: New Portfolio"+newPort);
        repRegion.put(new Integer("1"), newPort);
        //above call must be hooked in BEFORE_UPDATE_OP call.
      }
    });

    server.invoke(new CacheSerializableRunnable("query on server") {
      
      @Override
      public void run2() throws CacheException {
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        Position pos1 = null;
        while (!hooked){Wait.pause(100);}
        try {
          Object rs = qs.newQuery("<trace> select pos from /"+repRegionName+" p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
          CacheFactory.getAnyInstance().getLogger().fine("Shobhit: "+rs);
          assertTrue(rs instanceof SelectResults);
          pos1 = (Position) ((SelectResults)rs).iterator().next();
          if (!pos1.secId.equals("APPL")) {
            fail("Query thread did not verify index results even when RE is under update");
            IndexManager.testHook = null;       
          }          
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail("Query execution failed on server.", e);
          IndexManager.testHook = null;
        } finally {
          hooked = false;//Let client put go further.
        }
        while (!hooked) {
          Wait.pause(100);
        }
        try {
          Object rs = qs.newQuery("<trace> select pos from /"+repRegionName+" p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
          CacheFactory.getAnyInstance().getLogger().fine("Shobhit: "+rs);
          assertTrue(rs instanceof SelectResults);
          if (((SelectResults)rs).size() > 0) {
            Position pos2 = (Position) ((SelectResults)rs).iterator().next();
            if (pos2.equals(pos1)) {
              fail("Query thread did not verify index results even when RE is under update and " +
              "RegionEntry value has been modified before releasing the lock");
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("Query execution failed on server.");
        } finally {
          hooked = false;//Let client put go further.
          IndexManager.testHook = null;
        }
      }
    });
    ThreadUtils.join(putThread, 200);
  }

  @Category(FlakyTest.class) // GEODE-925: time sensitive, async actions, short timeouts
  @Test
  public void testRangeIndexWithIndexAndQueryFromCluaseMisMatch() { // TODO: fix misspelling
    // Create caches
    Properties props = new Properties();
    server.invoke(() -> PRClientServerTestBase.createCacheInVm( props ));

    server.invoke(new CacheSerializableRunnable("create indexes") {

      @Override
      public void run2() throws CacheException {
        cache = CacheFactory.getAnyInstance();
        Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
        IndexManager.testHook = null;
        // Create common Portfolios and NewPortfolios
        Position.cnt = 0;
        for (int j = cnt; j < cntDest; j++) {
          region.put(new Integer(j), new Portfolio(j));
        }
        
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        try {
          Index index = qs.createIndex("posIndex", "pos.secId", "/"+repRegionName+" p, p.collectionHolderMap.values coll, p.positions.values pos");
          assertEquals(12, index.getStatistics().getNumberOfKeys());
        } catch (Exception e) {
          fail("Index creation failed");
        }
      }
    });
    //Invoke update from client and stop in updateIndex
    //first before updating the RegionEntry and second after updating
    //the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync(new CacheSerializableRunnable("update a Region Entry") {
      
      @Override
      public void run2() throws CacheException {
        Region repRegion = CacheFactory.getAnyInstance().getRegion(repRegionName);
        IndexManager.testHook = new IndexManagerTestHook();
        // This portfolio with same ID must have different positions.
        repRegion.put(new Integer("1"), new Portfolio(1));
        //above call must be hooked in BEFORE_UPDATE_OP call.
      }
    });

    server.invoke(new CacheSerializableRunnable("query on server") {
      
      @Override
      public void run2() throws CacheException {
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        Position pos1 = null;
        while (!hooked){Wait.pause(100);}
        try {
          Object rs = qs.newQuery("<trace> select pos from /"+repRegionName+" p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
          CacheFactory.getAnyInstance().getLogger().fine("Shobhit: "+rs);
          assertTrue(rs instanceof SelectResults);
          pos1 = (Position) ((SelectResults)rs).iterator().next();
          if (!pos1.secId.equals("APPL")) {
            fail("Query thread did not verify index results even when RE is under update");
            IndexManager.testHook = null;       
          }          
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail("Query execution failed on server.", e);
          IndexManager.testHook = null;
        } finally {
          hooked = false;//Let client put go further.
        }
        while (!hooked) {
          Wait.pause(100);
        }
        try {
          Object rs = qs.newQuery("select pos from /"+repRegionName+" p, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
          assertTrue(rs instanceof SelectResults);
          if (((SelectResults)rs).size() > 0) {
            Position pos2 = (Position) ((SelectResults)rs).iterator().next();
            if (pos2.equals(pos1)) {
              fail("Query thread did not verify index results even when RE is under update and " +
              "RegionEntry value has been modified before releasing the lock");
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("Query execution failed on server.");
        } finally {
          hooked = false;//Let client put go further.
          IndexManager.testHook = null;
        }
      }
    });
    ThreadUtils.join(putThread, 200); // GEODE-925 occurs here and this is very short join 200 millis
  }

  @Test
  public void testRangeIndexWithIndexAndQueryFromCluaseMisMatch2() {
    // Create caches
    Properties props = new Properties();
    server.invoke(() -> PRClientServerTestBase.createCacheInVm( props ));

    server.invoke(new CacheSerializableRunnable("create indexes") {

      @Override
      public void run2() throws CacheException {
        cache = CacheFactory.getAnyInstance();
        Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
        IndexManager.testHook = null;
        // Create common Portfolios and NewPortfolios
        Position.cnt = 0;
        for (int j = cnt; j < cntDest; j++) {
          region.put(new Integer(j), new Portfolio(j));
        }
        
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        try {
          Index index = qs.createIndex("posIndex", "pos.secId", "/"+repRegionName+" p, p.positions.values pos");
          assertEquals(12, index.getStatistics().getNumberOfKeys());
        } catch (Exception e) {
          fail("Index creation failed");
        }
      }
    });
    //Invoke update from client and stop in updateIndex
    //first before updating the RegionEntry and second after updating
    //the RegionEntry.
    AsyncInvocation putThread = server.invokeAsync(new CacheSerializableRunnable("update a Region Entry") {
      
      @Override
      public void run2() throws CacheException {
        Region repRegion = CacheFactory.getAnyInstance().getRegion(repRegionName);
        IndexManager.testHook = new IndexManagerTestHook();
        // This portfolio with same ID must have different positions.
        repRegion.put(new Integer("1"), new Portfolio(1));
        //above call must be hooked in BEFORE_UPDATE_OP call.
      }
    });

    server.invoke(new CacheSerializableRunnable("query on server") {
      
      @Override
      public void run2() throws CacheException {
        QueryService qs = CacheFactory.getAnyInstance().getQueryService();
        Position pos1 = null;
        while (!hooked){Wait.pause(100);}
        try {
          Object rs = qs.newQuery("<trace> select pos from /"+repRegionName+" p, p.collectionHolderMap.values coll, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
          CacheFactory.getAnyInstance().getLogger().fine("Shobhit: "+rs);
          assertTrue(rs instanceof SelectResults);
          pos1 = (Position) ((SelectResults)rs).iterator().next();
          if (!pos1.secId.equals("APPL")) {
            fail("Query thread did not verify index results even when RE is under update");
            IndexManager.testHook = null;       
          }          
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail("Query execution failed on server.", e);
          IndexManager.testHook = null;
        } finally {
          hooked = false;//Let client put go further.
        }
        while (!hooked) {
          Wait.pause(100);
        }
        try {
          Object rs = qs.newQuery("select pos from /"+repRegionName+" p, p.collectionHolderMap.values coll, p.positions.values pos where pos.secId = 'APPL' AND p.ID = 1").execute();
          assertTrue(rs instanceof SelectResults);
          if (((SelectResults)rs).size() > 0) {
            Position pos2 = (Position) ((SelectResults)rs).iterator().next();
            if (pos2.equals(pos1)) {
              fail("Query thread did not verify index results even when RE is under update and " +
              "RegionEntry value has been modified before releasing the lock");
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("Query execution failed on server.");
        } finally {
          IndexManager.testHook = null;
          hooked = false;//Let client put go further.
        }
      }
    });
    ThreadUtils.join(putThread, 200);
  }
  
  public static void createProxyRegions() {
    new QueryDataInconsistencyDUnitTest().createProxyRegs();
  }

  private void createProxyRegs() {
    ClientCache cache = (ClientCache) CacheFactory.getAnyInstance();
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(
        repRegionName);

    /*cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(
        PartitionedRegionName1);*/
  }

  public static void createNewPR() {
    new QueryDataInconsistencyDUnitTest().createPR();
  }
  public void createPR() {
    PartitionResolver testKeyBasedResolver = new QueryAPITestPartitionResolver();
    cache = CacheFactory.getAnyInstance();
    cache
        .createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setPartitionAttributes(
            new PartitionAttributesFactory()
                .setTotalNumBuckets(numOfBuckets)
                .setPartitionResolver(testKeyBasedResolver)
                .create())
        .create(PartitionedRegionName1);
  }

  public static void createCacheClientWithoutRegion(String host, Integer port1) {
    new QueryDataInconsistencyDUnitTest().createCacheClientWithoutReg(
        host, port1);
  }

  private void createCacheClientWithoutReg(String host, Integer port1) {
    this.disconnectFromDS();
    ClientCache cache = new ClientCacheFactory().addPoolServer(host, port1)
        .create();
  }

  /**
   * This function puts portfolio objects into the created Region (PR or Local)
   * *
   * 
   * @param regionName
   * @param portfolio
   * @param to
   * @param from
   * @return cacheSerializable object
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRPuts(
      final String regionName, final Object[] portfolio, final int from,
      final int to) {
    SerializableRunnable puts = new CacheSerializableRunnable("Region Puts") {
      @Override
      public void run2() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(repRegionName);
        for (int j = from; j < to; j++)
          region.put(new Integer(j), portfolio[j]);
          LogWriterUtils.getLogWriter()
            .info(
                "PRQueryDUnitHelper#getCacheSerializableRunnableForPRPuts: Inserted Portfolio data on Region "
                    + regionName);
      }
    };
    return (CacheSerializableRunnable) puts;
  }

  public class IndexManagerTestHook implements org.apache.geode.cache.query.internal.index.IndexManager.TestHook{
    public void hook(final int spot) throws RuntimeException {
      switch (spot) {
      case 9: //Before Index update and after region entry lock.
        hooked  = true;
        LogWriterUtils.getLogWriter().info("QueryDataInconsistency.IndexManagerTestHook is hooked in Update Index Entry.");
        while(hooked) {
          Wait.pause(100);
        }
        assertEquals(hooked, false);
        break;
      case 10: //Before Region update and after Index Remove call.
        hooked  = true;
        LogWriterUtils.getLogWriter().info("QueryDataInconsistency.IndexManagerTestHook is hooked in Remove Index Entry.");
        while(hooked) {
          Wait.pause(100);
        }
        assertEquals(hooked, false);
        break;
      default:
        break;
      }
    }
  }
}
