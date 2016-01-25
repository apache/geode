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
package com.gemstone.gemfire.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
/**
 * verifies the count of clear operation
 *  
 * @author aingle
 */
public class CacheRegionClearStatsDUnitTest extends DistributedTestCase {
  /** the cache */
  private static GemFireCacheImpl cache = null;

  private static VM server1 = null;

  private static VM client1 = null;

  /** name of the test region */
  private static final String REGION_NAME = "CacheRegionClearStatsDUnitTest_Region";

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";
  
  private static final int clearOp = 2;
  
  /** constructor */
  public CacheRegionClearStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    client1 = host.getVM(1);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = (GemFireCacheImpl)CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1)
      throws Exception {
    new CacheRegionClearStatsDUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new CacheRegionClearStatsDUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host,
        port1.intValue()).setSubscriptionEnabled(false)
        .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(
            20000).setPingInterval(10000).setRetryAttempts(1)
        .create("CacheRegionClearStatsDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    //region.registerInterest("ALL_KEYS");
  }

  public static Integer createServerCacheDisk() throws Exception {
    return createCache(DataPolicy.PERSISTENT_REPLICATE);
  }

  private static Integer createCache(DataPolicy dataPolicy) throws Exception {
    new CacheRegionClearStatsDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(dataPolicy);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public static Integer createServerCache() throws Exception {
    return createCache(DataPolicy.REPLICATE);
  }

  public static void createClientCacheDisk(String host, Integer port1)
      throws Exception {
    new CacheRegionClearStatsDUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new CacheRegionClearStatsDUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host,
        port1.intValue()).setSubscriptionEnabled(false)
        .setThreadLocalConnections(true).setMinConnections(1).setReadTimeout(
            20000).setPingInterval(10000).setRetryAttempts(1).create(
            "CacheRegionClearStatsDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    //region.registerInterest("ALL_KEYS");
  }
  /**
   * This test does the following (<b> clear stats counter </b>):<br>
   * 1)Verifies that clear operation count matches with stats count<br>
   */
  public void testClearStatsWithNormalRegion(){
    Integer port1 = ((Integer)server1.invoke(
        CacheRegionClearStatsDUnitTest.class, "createServerCache"));

    client1.invoke(CacheRegionClearStatsDUnitTest.class,
        "createClientCache", new Object[] {
            getServerHostName(server1.getHost()), port1 });
    client1.invoke(CacheRegionClearStatsDUnitTest.class, "put");
    
    try{
      Thread.sleep(10000);
    }catch(Exception e){
      // sleep 
    }
    
    client1.invoke(CacheRegionClearStatsDUnitTest.class,
        "validationClearStat");
    
    server1.invoke(CacheRegionClearStatsDUnitTest.class,
    "validationClearStat");
  }
  /**
   * This test does the following (<b> clear stats counter when disk involved </b>):<br>
   * 1)Verifies that clear operation count matches with stats count <br>
   */
  public void testClearStatsWithDiskRegion(){
    Integer port1 = ((Integer)server1.invoke(
        CacheRegionClearStatsDUnitTest.class, "createServerCacheDisk"));

    client1.invoke(CacheRegionClearStatsDUnitTest.class,
        "createClientCacheDisk", new Object[] {
            getServerHostName(server1.getHost()), port1 });
    client1.invoke(CacheRegionClearStatsDUnitTest.class, "put");
    
    try{
      Thread.sleep(10000);
    }catch(Exception e){
      // sleep 
    }
    
    client1.invoke(CacheRegionClearStatsDUnitTest.class,
        "validationClearStat");
    
    server1.invoke(CacheRegionClearStatsDUnitTest.class,
    "validationClearStat");
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    client1.invoke(CacheRegionClearStatsDUnitTest.class, "closeCache");
    // then close the servers
    server1.invoke(CacheRegionClearStatsDUnitTest.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  public static void put() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);

      r1.put(k1, client_k1);
      assertEquals(r1.getEntry(k1).getValue(), client_k1);
      r1.put(k2, client_k2);
      assertEquals(r1.getEntry(k2).getValue(), client_k2);
      try{
        Thread.sleep(10000);
      }catch(Exception e){
        // sleep 
      }
      r1.clear();
      
      r1.put(k1, client_k1);
      assertEquals(r1.getEntry(k1).getValue(), client_k1);
      r1.put(k2, client_k2);
      assertEquals(r1.getEntry(k2).getValue(), client_k2);
      try{
        Thread.sleep(10000);
      }catch(Exception e){
        // sleep 
      }
      r1.clear();
    }
    catch (Exception ex) {
      fail("failed while put", ex);
    }
  }
  
  public static void validationClearStat(){
    assertEquals(cache.getCachePerfStats().getClearCount(), clearOp);
  }
  
  
}
