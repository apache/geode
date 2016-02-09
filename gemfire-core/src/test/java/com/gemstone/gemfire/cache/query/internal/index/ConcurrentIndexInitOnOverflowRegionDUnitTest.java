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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.io.IOException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * @author shobhit
 * 
 */
public class ConcurrentIndexInitOnOverflowRegionDUnitTest extends CacheTestCase {

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  String name;

  final int redundancy = 0;

  private int cnt = 0;

  private int cntDest = 1;

  public static volatile boolean hooked = false;

  private static int bridgeServerPort;
  
  /**
   * @param name
   */
  public ConcurrentIndexInitOnOverflowRegionDUnitTest(String name) {
    super(name);
  }

  /**
  *
  */
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnRR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(new CacheSerializableRunnable(
        "Create local region with synchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();
        Region partitionRegion = null;
        IndexManager.testHook = null;
        try {
          DiskStore ds = cache.findDiskStore("disk");
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs())
                .create("disk");
          }
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setIndexMaintenanceSynchronous(true);
          EvictionAttributesImpl evicAttr = new EvictionAttributesImpl()
              .setAction(EvictionAction.OVERFLOW_TO_DISK);
          evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
          attr.setEvictionAttributes(evicAttr);
          attr.setDataPolicy(DataPolicy.REPLICATE);
          // attr.setPartitionAttributes(new
          // PartitionAttributesFactory().setTotalNumBuckets(1).create());
          attr.setDiskStoreName("disk");
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          partitionRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", partitionRegion);
        assertTrue("Region ref claims to be destroyed",
            !partitionRegion.isDestroyed());
        // Create Indexes
        try {
          Index index = cache.getQueryService().createIndex("statusIndex",
              "p.status", "/" + name + " p");
          assertNotNull(index);
        } catch (Exception e1) {
          e1.printStackTrace();
          fail("Index creation failed");
        }
      }
    });

    // Start changing the value in Region which should turn into a deadlock if
    // the fix is not there
    AsyncInvocation asyncInv1 = vm0.invokeAsync(new CacheSerializableRunnable(
        "Change value in region") {

      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();

        // Do a put in region.
        Region r = PRQHelp.getCache().getRegion(name);

        for (int i = 0; i < 100; i++) {
          r.put(i, new PortfolioData(i));
        }

        assertNull(IndexManager.testHook);
        IndexManager.testHook = new IndexManagerTestHook();

        // Destroy one of the values.
        PRQHelp.getCache().getLogger().fine("Destroying the value");
        r.destroy(1);

        IndexManager.testHook = null;
      }
    });

    AsyncInvocation asyncInv2 = vm0.invokeAsync(new CacheSerializableRunnable(
        "Run query on region") {

      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();

        while (!hooked) {
          Wait.pause(100);
        }
        // Create and hence initialize Index
        try {
          Index index = cache.getQueryService().createIndex("idIndex",
              "p.ID", "/" + name + " p");
          assertNotNull(index);
        } catch (Exception e1) {
          e1.printStackTrace();
          fail("Index creation failed");
        }
      }
    });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
  }

  /**
  *
  */
  public void testAsyncIndexInitDuringEntryPutUsingClientOnRR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    IgnoredException.addIgnoredException("Unexpected IOException:");
    IgnoredException.addIgnoredException("java.net.SocketException");

    name = "PartionedPortfoliosPR";
    // Create Overflow Persistent Partition Region
    vm0.invoke(new CacheSerializableRunnable(
        "Create local region with synchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();
        
        Region partitionRegion = null;
        IndexManager.testHook = null;
        try {
          CacheServer bridge = cache.addCacheServer();
          bridge.setPort(0);
          bridge.start();
          bridgeServerPort = bridge.getPort();
          
          DiskStore ds = cache.findDiskStore("disk");
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs())
                .create("disk");
          }
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setIndexMaintenanceSynchronous(true);
          EvictionAttributesImpl evicAttr = new EvictionAttributesImpl()
              .setAction(EvictionAction.OVERFLOW_TO_DISK);
          evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
          attr.setEvictionAttributes(evicAttr);
          attr.setDataPolicy(DataPolicy.REPLICATE);
          // attr.setPartitionAttributes(new
          // PartitionAttributesFactory().setTotalNumBuckets(1).create());
          attr.setDiskStoreName("disk");
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          partitionRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
        } catch (IOException e) {
          e.printStackTrace();
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", partitionRegion);
        assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
        // Create Indexes
        try {
          Index index = cache.getQueryService().createIndex("idIndex",
              "p.ID", "/" + name + " p");
          assertNotNull(index);
        } catch (Exception e1) {
          e1.printStackTrace();
          fail("Index creation failed");
        }
      }
    });

    
    final int port = vm0.invokeInt(ConcurrentIndexInitOnOverflowRegionDUnitTest.class,
    "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Start changing the value in Region which should turn into a deadlock if
    // the fix is not there
    vm1.invoke(new CacheSerializableRunnable(
        "Change value in region") {

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        ClientCache clientCache = new ClientCacheFactory().addPoolServer(host0, port).create();

        // Do a put in region.
        Region r = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(name);

        for (int i = 0; i < 100; i++) {
          r.put(i, new PortfolioData(i));
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Set Test Hook") {
      
      @Override
      public void run2() throws CacheException {
        // Set test hook before client operation
        assertNull(IndexManager.testHook);
        IndexManager.testHook = new IndexManagerTestHook();
      }
    });

    AsyncInvocation asyncInv1 = vm1.invokeAsync(new CacheSerializableRunnable("Change value in region") {

      @Override
      public void run2() throws CacheException {
        ClientCache clientCache = ClientCacheFactory.getAnyInstance();

        // Do a put in region.
        Region r = clientCache.getRegion(name);

        // Destroy one of the values.
        clientCache.getLogger().fine("Destroying the value");
        r.destroy(1);
      }
    });

    AsyncInvocation asyncInv2 = vm0.invokeAsync(new CacheSerializableRunnable(
        "Run query on region") {

      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();

        while (!hooked) {
          Wait.pause(100);
        }
        // Create Indexes
        try {
          Index index = cache.getQueryService().createIndex("statusIndex",
              "p.status", "/" + name + " p");
          assertNotNull(index);
        } catch (Exception e1) {
          e1.printStackTrace();
          fail("Index creation failed");
        }
      }
    });

    // If we take more than 30 seconds then its a deadlock.
    ThreadUtils.join(asyncInv2, 30 * 1000);
    ThreadUtils.join(asyncInv1, 30 * 1000);
    
    vm0.invoke(new CacheSerializableRunnable("Set Test Hook") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(IndexManager.testHook);
        IndexManager.testHook = null;
      }
    });

  }

  /**
   * This tests if index updates are blocked while region.clear() is
   * called and indexes are being reinitialized.
   */
  public void testIndexUpdateWithRegionClear() {
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    final String regionName = "portfolio";
    
    hooked = false;
    
    // Create region and an index on it
    vm0.invoke(new CacheSerializableRunnable("Create region and index") {
      
      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();
        Region region = cache.createRegionFactory(RegionShortcut.LOCAL).create(regionName);
        QueryService qService = cache.getQueryService();
        
        try {
          qService.createIndex("idIndex", "ID", "/"+regionName);
          qService.createIndex("secIdIndex", "pos.secId", "/" + regionName + " p, p.positions.values pos");
        } catch (Exception e) {
          fail("Index creation failed." + e);
        }
      }
    });

    final class LocalTestHook implements TestHook {

      @Override
      public void hook(int spot) throws RuntimeException {
        switch (spot) {
        case 6: // processAction in IndexManager
          hooked = true;
          //wait untill some thread unhooks.
          while (hooked) { Wait.pause(20); }
          break;
        default:
          break;
        }
      }
      
    }
    
    // Asynch invocation for continuous index updates
    AsyncInvocation indexUpdateAsysnch = vm0.invokeAsync(new CacheSerializableRunnable("index updates") {
      
      @Override
      public void run2() throws CacheException {
        
        Region region = PRQHelp.getCache().getRegion(regionName);
        for (int i=0; i<100; i++) {
          if (i == 50) IndexManager.testHook = new LocalTestHook();
          region.put(i, new Portfolio(i));
          if (i == 50) Wait.pause(20);
        }
      }
    });
 
    // Region.clear() which should block other region updates.
    vm0.invoke(new CacheSerializableRunnable("Clear the region") {
      
      @Override
      public void run2() throws CacheException {
        Region region = PRQHelp.getCache().getRegion(regionName);
        
        while(!hooked) {
          Wait.pause(100);
        }
        if (hooked) {
          hooked = false;
          IndexManager.testHook = null;
          region.clear();
        }

        try {
            QueryService qservice = PRQHelp.getCache().getQueryService();
            Index index = qservice.getIndex(region, "idIndex");
            if (((CompactRangeIndex)index).getIndexStorage().size() > 1) {
              fail("After clear region size is supposed to be zero as all index updates are blocked. Current region size is: "+ region.size());
            }
        } finally {
          IndexManager.testHook = null;  
        }
      }
    });

    // Kill asynch thread
    ThreadUtils.join(indexUpdateAsysnch, 20000);

    //Verify region size which must be 50
    vm0.invoke(new CacheSerializableRunnable("Check region size") {
      
      @Override
      public void run2() throws CacheException {
        Region region = PRQHelp.getCache().getRegion(regionName);
        if (region.size() > 50) {
          fail("After clear region size is supposed to be 50 as all index updates are blocked " + region.size());
        }
      }
    });
  }

  public class IndexManagerTestHook implements
      com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook {
    public void hook(final int spot) throws RuntimeException {
      switch (spot) {
      case 6: // Before Index update and after region entry lock.
        hooked = true;
        LogWriterUtils.getLogWriter().fine("IndexManagerTestHook is hooked.");
        Wait.pause(10000);
        hooked = false;
        break;
      default:
        break;
      }
    }
  }

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }
}
