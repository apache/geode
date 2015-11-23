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
package com.gemstone.gemfire.internal;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.stats50.VMStats50;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * 
 * @author jhuynh
 *
 */
public class FDDUnitTest extends CacheTestCase {

  VM vm0;
  VM vm1;
  VM vm2;

  
  public FDDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    //getSystem();
   
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
  }
  
  public void tearDown2() throws Exception {
    vm0.invoke(cleanup());
    vm1.invoke(cleanup());
    vm2.invoke(cleanup());
    vm0.invoke(CacheServerTestUtil.class, "closeCache");
    vm1.invoke(CacheServerTestUtil.class, "closeCache");
    vm2.invoke(CacheServerTestUtil.class, "closeCache");
  }

  public void testEmpty() {
    //Ticket #GEODE-338.  Disable the test for now and rewrite as a junit test.
  }
  
  public void disable_testFDSocketFixOnlyServers() throws Exception {
    String os = System.getProperty("os.name");
    if (os != null) {
      if (os.indexOf("Windows") != -1) {
        System.out.println("This test is disabled on Windows");
        //we are running this test on windows.  fd stats are not available in windows so let's
        //just not run this test
        return;
      }
    }
    try {
      StringBuffer incaseOfFailure = new StringBuffer();
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(3);
      int numThreads = 30;

      startCacheServer(vm0, port[0]);
      startCacheServer(vm1, port[1]);
      startCacheServer(vm2, port[2]);

      createRegion(vm0, "portfolios", RegionShortcut.PARTITION_REDUNDANT);
      createRegion(vm1, "portfolios", RegionShortcut.PARTITION_REDUNDANT);
      createRegion(vm2, "portfolios", RegionShortcut.PARTITION_REDUNDANT);

      // run test without selector pooling
      setUseSelectorPooling(vm0, false);
      long startingFDs = checkFD(vm0);
      doPuts(vm0, numThreads, "portfolios");
      long endFDs = checkFD(vm0);
      long numFDs = endFDs - startingFDs;
      incaseOfFailure.append("NoSelectorPooling startFDs: " + startingFDs + " endFDs: " + endFDs + " diff:" + numFDs + " ");

      // run test with selector pooling
      setUseSelectorPooling(vm0, true);
      long startingFDsWithPooling = checkFD(vm0);
      doPuts(vm0, numThreads, "portfolios");
      long endFDsWithPooling = checkFD(vm0);
      long numFDsWithPooling = endFDsWithPooling - startingFDsWithPooling;
      incaseOfFailure.append("SelectorPooling#1 startFDs: " + startingFDsWithPooling + " endFDs: " + endFDsWithPooling + " diff:" + numFDsWithPooling + " ");
      assertTrue(incaseOfFailure.toString(), numFDsWithPooling < numFDs);

      // run it again and see if the number still is below
      startingFDsWithPooling = checkFD(vm0);
      doPuts(vm0, numThreads, "portfolios");
      endFDsWithPooling = checkFD(vm0);
      numFDsWithPooling = endFDsWithPooling - startingFDsWithPooling;
      // if you see these asserts failing, it could be that we are not using the
      // selector pool
      incaseOfFailure.append("SelectorPooling#2 startFDs: " + startingFDsWithPooling + " endFDs: " + endFDsWithPooling + " diff:" + numFDsWithPooling + " ");
      assertTrue(incaseOfFailure.toString(), numFDsWithPooling < numFDs);

    } finally {
      setUseSelectorPooling(vm0, true);
    }

  }
  
  private void setUseSelectorPooling(VM vm, final boolean useSelectorPooling) {
    vm.invoke(new SerializableRunnable("setting use selectorPooling to " + useSelectorPooling) {
      public void run() {
        SocketUtils.USE_SELECTOR_POOLING = useSelectorPooling;
      }
    });
  }
  private Long checkFD(VM vm) {
    return (Long)vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        VMStatsContract stats = ((InternalDistributedSystem)system).getStatSampler().getVMStats();
        VMStats50 vmstats = (VMStats50) stats;
        return vmstats.getVMStats().get("fdsOpen").longValue();
      }
    });
  }
  
  private void doPuts(VM vm, final int numThreads, final String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region region = getCache().getRegion(regionName);
        if (region == null) {
          throw new Exception("No Region found");
        }
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
          for (int i = 0; i < numThreads; i++) {
            executor.execute(new Runnable() {
              public void run() {
                  for (int i = 0; i < 10; i++) {
                    String myValue = "string" + i;
                    region.put("k" + i, myValue);
                    try {
                      Thread.sleep(75); 
                    }
                    catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    
                  }
              }
            });
          }
        
          executor.shutdown();
       return executor.awaitTermination(90, TimeUnit.SECONDS);
      }
    });
  }
  
  private String createGarbage(int valueIndex) {
    StringBuffer[] randomStringArray = new StringBuffer[100];
    for (int i = 0; i < randomStringArray.length; i++) {
      randomStringArray[i] = new StringBuffer();
      randomStringArray[i].append("value" + valueIndex + "," + Math.random());
    }
    return randomStringArray[(int)(Math.random() * randomStringArray.length)].toString();
  }
 
  private void createRegion(VM vm, final String regionName, final RegionShortcut shortcut) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        RegionFactory factory = getCache().createRegionFactory(shortcut)
            .setPartitionAttributes(paf.create());
        factory.create(regionName);
        return null;
      }
    });
  }
  
  private void createRegionOnClient(VM vm, final String regionName, final ClientRegionShortcut shortcut) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        ClientRegionFactory factory = ((ClientCache)getCache()).createClientRegionFactory(shortcut);
        factory.create(regionName);
        return null;
      }
    });
  }
  
  private void startCacheServer(VM server, final int port) throws Exception {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        //System.setProperty("IDLE_THREAD_TIMEOUT", "50");
        disconnectFromDS();
        
        getSystem(getServerProperties());
        
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        
        CacheServer cacheServer = cache.addCacheServer();
        cacheServer.setPort(port);
        cacheServer.start();  
        return null;
      }
    });
  }
  
  private void startClient(VM client, final VM server, final int port) {
    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        //System.setProperty("IDLE_THREAD_TIMEOUT", "50");
        Properties props = getClientProps();
        getSystem(props);
        
        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(server.getHost()), port);
        ccf.setPoolSubscriptionEnabled(true);
        
        ClientCache cache = (ClientCache)getClientCache(ccf);
      }
    });
  }
  
  private Runnable cleanup() {
    return new SerializableRunnable() {
      public void run() {
        //System.setProperty("IDLE_THREAD_TIMEOUT", "30000*60");
      }
    };
  }
 
  protected Properties getClientProps() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    p.setProperty(DistributionConfig.CONSERVE_SOCKETS_NAME, "false");
    return p;
  }

  protected Properties getServerProperties() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["+getDUnitLocatorPort()+"]");
    p.setProperty(DistributionConfig.CONSERVE_SOCKETS_NAME, "false");
    //p.setProperty(DistributionConfig.SOCKET_LEASE_TIME_NAME, "120000");
    return p;
  }
 

}
