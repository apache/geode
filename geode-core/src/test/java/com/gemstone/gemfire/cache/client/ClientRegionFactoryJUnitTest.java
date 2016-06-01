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
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.internal.ProxyRegion;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Properties;

import static com.gemstone.gemfire.cache.client.ClientRegionShortcut.*;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Unit test for the ClientRegionFactory class
 * @since GemFire 6.5
 */
@Category(IntegrationTest.class)
public class ClientRegionFactoryJUnitTest {

  @Rule
  public TestName testName = new TestName();
  
  private static final String key = "key";
  private static final Integer val = new Integer(1);
  private final String r1Name = "r1";
  private final String sr1Name = "sr1";
  private final String r2Name = "r2";
  private final String r3Name = "r3";
  
  private Cache cache;
  private DistributedSystem distSys;
  
  private Region r1;
  private Region r2;
  private Region r3;
  private Region sr1;

  @After
  public void tearDown() throws Exception {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null && ids.isConnected()) {
      if (r1 != null) {
        this.cleanUpRegion(r1);
      }
      if (r2 != null) {
        this.cleanUpRegion(r2);
      }
      if (r3 != null) {
        this.cleanUpRegion(r3);
      }
      if (sr1 != null) {
        this.cleanUpRegion(sr1);
      }
      ids.disconnect();
    }
    this.distSys = null;
    this.cache = null;
  }

  @Test
  public void testLOCAL() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
  }
  
  @Test
  public void testLOCAL_HEAP_LRU() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_HEAP_LRU);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage(),
                 0);
  }
  
  @Test
  public void testLOCAL_OVERFLOW() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_OVERFLOW);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage(),
                 0);
  }
  
  @Test
  public void testLOCAL_PERSISTENT() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
  }
  
  @Test
  public void testLOCAL_PERSISTENT_OVERFLOW() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT_OVERFLOW);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage(),
                 0);
  }
  
  @Test
  public void testPROXY() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
  }
  
  @Test
  public void testCACHING_PROXY() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(0,
                 (int)c.getResourceManager().getEvictionHeapPercentage());
  }
  
  @Test
  public void testCACHING_PROXY_LRU() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY_HEAP_LRU);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage(),
                 0);
  }
  
  @Test
  public void testCACHING_PROXY_OVERFLOW() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY_OVERFLOW);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage(),
                 0);
  }

  @Test
  public void testAddCacheListener() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
    CacheListener cl = new MyCacheListener();
    r1 = factory.addCacheListener(cl).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(cl, ra.getCacheListener());
  }

  @Test
  public void testInitCacheListener() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
    CacheListener cl1 = new MyCacheListener();
    CacheListener cl2 = new MyCacheListener();
    r1 = factory.initCacheListeners(new CacheListener[] {cl1, cl2}).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, Arrays.equals(new CacheListener[] {cl1, cl2}, ra.getCacheListeners()));
  }

  @Test
  public void testSetEvictionAttributes() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(77)).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(EvictionAttributes.createLRUEntryAttributes(77), ra.getEvictionAttributes());
  }

  @Test
  public void testSetEntryIdleTimeout() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setEntryIdleTimeout(ea).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getEntryIdleTimeout());
  }

  @Test
  public void testSetCustomEntryIdleTimeout() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    MyCustomExpiry ce = new MyCustomExpiry();
    r1 = factory.setCustomEntryIdleTimeout(ce).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ce, ra.getCustomEntryIdleTimeout());
  }

  @Test
  public void testSetEntryTimeToLive() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setEntryTimeToLive(ea).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getEntryTimeToLive());
  }

  @Test
  public void testSetCustomEntryTimeToLive() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    MyCustomExpiry ce = new MyCustomExpiry();
    r1 = factory.setCustomEntryTimeToLive(ce).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ce, ra.getCustomEntryTimeToLive());
  }

  @Test
  public void testSetRegionIdleTimeout() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setRegionIdleTimeout(ea).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getRegionIdleTimeout());
  }

  @Test
  public void testSetRegionTimeToLive() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setRegionTimeToLive(ea).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(ea, ra.getRegionTimeToLive());
  }

  @Test
  public void testSetKeyConstraint() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setKeyConstraint(String.class).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(String.class, ra.getKeyConstraint());
  }

  @Test
  public void testSetValueConstraint() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setValueConstraint(String.class).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(String.class, ra.getValueConstraint());
  }

  @Test
  public void testSetInitialCapacity() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setInitialCapacity(777).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(777, ra.getInitialCapacity());
  }

  @Test
  public void testSetLoadFactor() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setLoadFactor(77.7f).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(77.7f, ra.getLoadFactor(), 0);
  }

  @Test
  public void testSetConcurrencyLevel() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setConcurrencyLevel(7).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(7, ra.getConcurrencyLevel());
  }

  @Test
  public void testSetDiskStoreName() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    c.createDiskStoreFactory().create("ds");
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.setDiskStoreName("ds").create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals("ds", ra.getDiskStoreName());
  }

  @Test
  public void testSetDiskSynchronous() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.setDiskSynchronous(true).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.isDiskSynchronous());
  }

  @Test
  public void testSetStatisticsEnabled() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setStatisticsEnabled(true).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getStatisticsEnabled());
  }

  @Test
  public void testSetCloningEnabled() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setCloningEnabled(true).create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(true, ra.getCloningEnabled());
  }

  @Test
  public void testSetPoolName() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
    r1 = factory.setPoolName("DEFAULT").create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals("DEFAULT", ra.getPoolName());
  }

  @Test
  public void testMultiUserRootRegions() throws Exception {
    DistributedSystem ds = DistributedSystem.connect(createGemFireProperties());
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777).setMultiuserAuthentication(true).create("muPool");
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 6666).create("suPool");
    ClientCache cc = new ClientCacheFactory().create();
    cc.createClientRegionFactory(PROXY).setPoolName("muPool").create("p");
    cc.createClientRegionFactory(CACHING_PROXY).setPoolName("suPool").create("cp");
    cc.createClientRegionFactory(LOCAL).create("l");
    assertEquals(3, cc.rootRegions().size());

    {
      Properties muProps = new Properties();
      muProps.setProperty("user", "foo");
      RegionService rs = cc.createAuthenticatedView(muProps, "muPool");
      assertNotNull(rs.getRegion("p"));
      try {
        rs.getRegion("cp");
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      try {
        rs.getRegion("l");
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      assertEquals(1, rs.rootRegions().size());
      assertEquals(true, rs.getRegion("p") instanceof ProxyRegion);
      assertEquals(true, rs.rootRegions().iterator().next() instanceof ProxyRegion);
    }
  }

  /**
   * Make sure getLocalQueryService works.
   */
  @Test
  public void testBug42294() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    QueryService qs = c.getLocalQueryService();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
    r1 = factory.create("localRegion");
    Query q = qs.newQuery("SELECT * from /localRegion");
    q.execute();
  }
  
  @Test
  public void testSubregionCreate() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
    r1 = factory.create(this.r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    
    sr1 = factory.createSubregion(r1, sr1Name);
    RegionAttributes sr1ra = sr1.getAttributes();
    assertEquals(DataPolicy.NORMAL, sr1ra.getDataPolicy());
    assertEquals(Scope.LOCAL, sr1ra.getScope());
    assertEquals(null, sr1ra.getPoolName());
    
    try {
      factory.createSubregion(r1, sr1Name);
      fail("Expected RegionExistsException");
    } catch (RegionExistsException expected) {
    }
    cleanUpRegion(sr1);
    cleanUpRegion(r1);
    try {
      factory.createSubregion(r1, sr1Name);
      fail("Expected RegionDestroyedException");
    } catch (RegionDestroyedException expected) {
    }
  }
  
  private String getName() {
    return getClass().getSimpleName() + "_" + this.testName.getMethodName();
  }
  
  private Properties createGemFireProperties() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    return props;
  }
  
  private void cleanUpRegion(Region r) {
    if (r != null && !r.getCache().isClosed() && !r.isDestroyed()
        && r.getCache().getDistributedSystem().isConnected()) {
      this.cache = r.getCache();
      this.distSys = this.cache.getDistributedSystem();
      r.localDestroyRegion();
    }
  }

  private void assertBasicRegionFunctionality(Region r, String name) {
    assertEquals(r.getName(), name);
    r.put(key, val);
    assertEquals(r.getEntry(key).getValue(), val);
  }

  private static void assertRegionAttributes(RegionAttributes ra1, RegionAttributes ra2) {
    assertEquals(ra1.getScope(), ra2.getScope());
    assertEquals(ra1.getKeyConstraint(), ra2.getKeyConstraint());
    assertEquals(ra1.getValueConstraint(), ra2.getValueConstraint());
    assertEquals(ra1.getCacheListener(), ra2.getCacheListener());
    assertEquals(ra1.getCacheWriter(), ra2.getCacheWriter());
    assertEquals(ra1.getCacheLoader(), ra2.getCacheLoader());
    assertEquals(ra1.getStatisticsEnabled(), ra2.getStatisticsEnabled());
    assertEquals(ra1.getConcurrencyLevel(), ra2.getConcurrencyLevel());
    assertEquals(ra1.getInitialCapacity(), ra2.getInitialCapacity());
    assertTrue(ra1.getLoadFactor() == ra2.getLoadFactor());
    assertEquals(ra1.getEarlyAck(), ra2.getEarlyAck());
    assertEquals(ra1.isDiskSynchronous(), ra2.isDiskSynchronous());
    assertEquals(ra1.getDiskStoreName(), ra2.getDiskStoreName());
  }

  public static class MyCacheListener extends CacheListenerAdapter {
  }

  public static class MyCustomExpiry implements CustomExpiry {
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return null;
    }
    public void close() {
    }
  }
}
