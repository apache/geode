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
package com.gemstone.gemfire.cache.query.dunit;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.execute.PRClientServerTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This tests the data inconsistency during update on an index and querying the
 * same UNLOCKED index.
 * 
 * @author shobhit
 * 
 */
public class QueryDataInconsistencyDUnitTest extends CacheTestCase {

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

  private static PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  public static volatile boolean hooked = false;
  /**
   * @param name
   */
  public QueryDataInconsistencyDUnitTest(String name) {
    super(name);
  }

  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(CacheTestCase.class, "disconnectFromDS");
    super.tearDown2();
    invokeInEveryVM(QueryObserverHolder.class, "reset");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server = host.getVM(0);
  }

  public void testCompactRangeIndex() {
    // Create caches
    Properties props = new Properties();
    server.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

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
        while (!hooked){pause(100);}
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
        while (!hooked){pause(100);}
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
    DistributedTestCase.join(putThread, 200, this.getLogWriter());
  }

  public void testRangeIndex() {
    // Create caches
    Properties props = new Properties();
    server.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

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
        while (!hooked){pause(100);}
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
          fail("Query execution failed on server.", e);
          IndexManager.testHook = null;
        } finally {
          hooked = false;//Let client put go further.
        }
        while (!hooked) {
          pause(100);
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
    DistributedTestCase.join(putThread, 200, this.getLogWriter());
  }
  
  public void testRangeIndexWithIndexAndQueryFromCluaseMisMatch() {
    // Create caches
    Properties props = new Properties();
    server.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

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
        while (!hooked){pause(100);}
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
          fail("Query execution failed on server.", e);
          IndexManager.testHook = null;
        } finally {
          hooked = false;//Let client put go further.
        }
        while (!hooked) {
          pause(100);
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
    DistributedTestCase.join(putThread, 200, this.getLogWriter());
  }

  public void testRangeIndexWithIndexAndQueryFromCluaseMisMatch2() {
    // Create caches
    Properties props = new Properties();
    server.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

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
        while (!hooked){pause(100);}
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
          fail("Query execution failed on server.", e);
          IndexManager.testHook = null;
        } finally {
          hooked = false;//Let client put go further.
        }
        while (!hooked) {
          pause(100);
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
    DistributedTestCase.join(putThread, 200, this.getLogWriter());
  }
  
  public static void createProxyRegions() {
    new QueryDataInconsistencyDUnitTest("temp").createProxyRegs();
  }

  private void createProxyRegs() {
    ClientCache cache = (ClientCache) CacheFactory.getAnyInstance();
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(
        repRegionName);

    /*cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(
        PartitionedRegionName1);*/
  }

  public static void createNewPR() {
    new QueryDataInconsistencyDUnitTest("temp").createPR();
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
    new QueryDataInconsistencyDUnitTest("temp").createCacheClientWithoutReg(
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
          getLogWriter()
            .info(
                "PRQueryDUnitHelper#getCacheSerializableRunnableForPRPuts: Inserted Portfolio data on Region "
                    + regionName);
      }
    };
    return (CacheSerializableRunnable) puts;
  }

  public class IndexManagerTestHook implements com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook{
    public void hook(final int spot) throws RuntimeException {
      switch (spot) {
      case 9: //Before Index update and after region entry lock.
        hooked  = true;
        getLogWriter().info("QueryDataInconsistency.IndexManagerTestHook is hooked in Update Index Entry.");
        while(hooked) {
          pause(100);
        }
        assertEquals(hooked, false);
        break;
      case 10: //Before Region update and after Index Remove call.
        hooked  = true;
        getLogWriter().info("QueryDataInconsistency.IndexManagerTestHook is hooked in Remove Index Entry.");
        while(hooked) {
          pause(100);
        }
        assertEquals(hooked, false);
        break;
      default:
        break;
      }
    }
  }
}
