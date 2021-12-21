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
package org.apache.geode.cache.client;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY_HEAP_LRU;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY_OVERFLOW;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL_HEAP_LRU;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL_OVERFLOW;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL_PERSISTENT;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.ProxyRegion;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Unit test for the ClientRegionFactory class
 *
 * @since GemFire 6.5
 */
@Category(ClientServerTest.class)
public class ClientRegionFactoryJUnitTest {

  @Rule
  public TestName testName = new TestName();

  private Region r1;
  private Region sr1;
  private Cache cache;
  private DistributedSystem distSys;
  private final String r1Name = "r1";

  @After
  public void tearDown() throws Exception {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null && ids.isConnected()) {
      if (r1 != null) {
        cleanUpRegion(r1);
      }
      if (sr1 != null) {
        cleanUpRegion(sr1);
      }

      ids.disconnect();
    }
    distSys = null;
    cache = null;
  }

  @Test
  public void testLOCAL() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isNull();
  }

  @Test
  public void testLOCAL_HEAP_LRU() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_HEAP_LRU);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isNull();
    assertThat(ra.getEvictionAttributes()).isEqualTo(EvictionAttributes.createLRUHeapAttributes());
    assertThat(c.getResourceManager().getEvictionHeapPercentage())
        .isEqualTo(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE);
  }

  @Test
  public void testLOCAL_OVERFLOW() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isNull();
    assertThat(ra.getEvictionAttributes()).isEqualTo(
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
    assertThat(c.getResourceManager().getEvictionHeapPercentage())
        .isEqualTo(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE);
  }

  @Test
  public void testLOCAL_PERSISTENT() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.PERSISTENT_REPLICATE);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isNull();
  }

  @Test
  public void testLOCAL_PERSISTENT_OVERFLOW() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.PERSISTENT_REPLICATE);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isNull();
    assertThat(ra.getEvictionAttributes()).isEqualTo(
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
    assertThat(c.getResourceManager().getEvictionHeapPercentage())
        .isEqualTo(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE);
  }

  @Test
  public void testPROXY() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.EMPTY);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isEqualTo("DEFAULT");
  }

  @Test
  public void testCACHING_PROXY() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isEqualTo("DEFAULT");
    assertThat(c.getResourceManager().getEvictionHeapPercentage()).isEqualTo(0);
  }

  @Test
  public void testCACHING_PROXY_LRU() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY_HEAP_LRU);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isEqualTo("DEFAULT");
    assertThat(ra.getEvictionAttributes())
        .isEqualTo(EvictionAttributes.createLRUHeapAttributes());
    assertThat(c.getResourceManager().getEvictionHeapPercentage())
        .isEqualTo(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE);
  }

  @Test
  public void testCACHING_PROXY_OVERFLOW() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY_OVERFLOW);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isEqualTo("DEFAULT");
    assertThat(ra.getEvictionAttributes()).isEqualTo(
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
    assertThat(c.getResourceManager().getEvictionHeapPercentage())
        .isEqualTo(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE);
  }

  @Test
  public void testAddCacheListener() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<Object, Object> factory = c.createClientRegionFactory(PROXY);
    CacheListener<Object, Object> cl = new MyCacheListener();
    r1 = factory.addCacheListener(cl).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getCacheListeners()[0]).isEqualTo(cl);
  }

  @Test
  public void testInitCacheListener() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<Object, Object> factory = c.createClientRegionFactory(PROXY);
    CacheListener<Object, Object> cl1 = new MyCacheListener();
    CacheListener<Object, Object> cl2 = new MyCacheListener();
    r1 = factory.initCacheListeners(new CacheListener[] {cl1, cl2}).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(Arrays.equals(new CacheListener[] {cl1, cl2}, ra.getCacheListeners())).isTrue();
  }

  @Test
  public void testSetEvictionAttributes() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(77))
        .create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getEvictionAttributes())
        .isEqualTo(EvictionAttributes.createLRUEntryAttributes(77));
  }

  @Test
  public void testSetEntryIdleTimeout() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setEntryIdleTimeout(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getEntryIdleTimeout()).isEqualTo(ea);
  }

  @Test
  public void testSetCustomEntryIdleTimeout() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<Object, Object> factory = c.createClientRegionFactory(CACHING_PROXY);
    MyCustomExpiry ce = new MyCustomExpiry();
    r1 = factory.setCustomEntryIdleTimeout(ce).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getCustomEntryIdleTimeout()).isEqualTo(ce);
  }

  @Test
  public void testSetEntryTimeToLive() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setEntryTimeToLive(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getEntryTimeToLive()).isEqualTo(ea);
  }

  @Test
  public void testSetCustomEntryTimeToLive() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<Object, Object> factory = c.createClientRegionFactory(CACHING_PROXY);
    MyCustomExpiry ce = new MyCustomExpiry();
    r1 = factory.setCustomEntryTimeToLive(ce).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getCustomEntryTimeToLive()).isEqualTo(ce);
  }

  @Test
  public void testSetRegionIdleTimeout() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setRegionIdleTimeout(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getRegionIdleTimeout()).isEqualTo(ea);
  }

  @Test
  public void testSetRegionTimeToLive() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    ExpirationAttributes ea = new ExpirationAttributes(7);
    r1 = factory.setRegionTimeToLive(ea).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getRegionTimeToLive()).isEqualTo(ea);
  }

  @Test
  public void testSetKeyConstraint() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<String, String> factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setKeyConstraint(String.class).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getKeyConstraint()).isEqualTo(String.class);
  }

  @Test
  public void testSetValueConstraint() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<String, String> factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setValueConstraint(String.class).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getValueConstraint()).isEqualTo(String.class);
  }

  @Test
  public void testSetInitialCapacity() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setInitialCapacity(777).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getInitialCapacity()).isEqualTo(777);
  }

  @Test
  public void testSetLoadFactor() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setLoadFactor(77.7f).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getLoadFactor()).isEqualTo(77.7f);
  }

  @Test
  public void testSetConcurrencyLevel() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setConcurrencyLevel(7).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getConcurrencyLevel()).isEqualTo(7);
  }

  @Test
  public void testSetDiskStoreName() {
    ClientCache c = new ClientCacheFactory().create();
    c.createDiskStoreFactory().create("ds");
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.setDiskStoreName("ds").create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDiskStoreName()).isEqualTo("ds");
  }

  @Test
  public void testSetDiskSynchronous() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
    r1 = factory.setDiskSynchronous(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.isDiskSynchronous()).isTrue();
  }

  @Test
  public void testSetStatisticsEnabled() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setStatisticsEnabled(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getStatisticsEnabled()).isTrue();
  }

  @Test
  public void testSetCloningEnabled() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
    r1 = factory.setCloningEnabled(true).create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getCloningEnabled()).isTrue();
  }

  @Test
  public void testSetPoolName() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
    r1 = factory.setPoolName("DEFAULT").create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getPoolName()).isEqualTo("DEFAULT");
  }

  @Test
  public void testMultiUserRootRegions() throws Exception {
    DistributedSystem.connect(createGemFireProperties());
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777)
        .setMultiuserAuthentication(true).create("muPool");
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 6666)
        .create("suPool");
    ClientCache cc = new ClientCacheFactory().create();
    cc.createClientRegionFactory(PROXY).setPoolName("muPool").create("p");
    cc.createClientRegionFactory(CACHING_PROXY).setPoolName("suPool").create("cp");
    cc.createClientRegionFactory(LOCAL).create("l");
    assertThat(cc.rootRegions().size()).isEqualTo(3);

    {
      Properties muProps = new Properties();
      muProps.setProperty("user", "foo");
      RegionService rs = cc.createAuthenticatedView(muProps, "muPool");
      assertThat(rs.getRegion("p")).isNotNull();
      assertThatThrownBy(() -> rs.getRegion("cp")).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> rs.getRegion("l")).isInstanceOf(IllegalStateException.class);
      assertThat(rs.rootRegions().size()).isEqualTo(1);
      assertThat(rs.getRegion("p")).isInstanceOf(ProxyRegion.class);
      assertThat(rs.rootRegions().iterator().next()).isInstanceOf(ProxyRegion.class);
    }
  }

  /**
   * Make sure getLocalQueryService works.
   */
  @Test
  public void testBug42294() {
    ClientCache c = new ClientCacheFactory().create();
    QueryService qs = c.getLocalQueryService();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
    r1 = factory.create("localRegion");
    Query q = qs.newQuery("SELECT * from " + SEPARATOR + "localRegion");
    assertThatCode(q::execute).doesNotThrowAnyException();
  }

  @Test
  public void testSubregionCreate() {
    ClientCache c = new ClientCacheFactory().create();
    ClientRegionFactory<Object, Object> factory = c.createClientRegionFactory(LOCAL);
    r1 = factory.create(r1Name);
    RegionAttributes ra = r1.getAttributes();
    assertThat(ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(ra.getPoolName()).isNull();

    String sr1Name = "sr1";
    sr1 = factory.createSubregion(r1, sr1Name);
    RegionAttributes sr1ra = sr1.getAttributes();
    assertThat(sr1ra.getDataPolicy()).isEqualTo(DataPolicy.NORMAL);
    assertThat(sr1ra.getScope()).isEqualTo(Scope.LOCAL);
    assertThat(sr1ra.getPoolName()).isNull();
    assertThatThrownBy(() -> factory.createSubregion(r1, sr1Name))
        .isInstanceOf(RegionExistsException.class);

    cleanUpRegion(sr1);
    cleanUpRegion(r1);
    assertThatThrownBy(() -> factory.createSubregion(r1, sr1Name))
        .isInstanceOf(RegionDestroyedException.class);
  }

  @Test
  public void setPoolNameShouldThrowExceptionWhenPoolDoesNotExist() throws Exception {
    DistributedSystem.connect(createGemFireProperties());
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777)
        .create("poolOne");
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 6666)
        .create("poolTwo");

    ClientCache cc = new ClientCacheFactory().create();
    assertThat(cc.createClientRegionFactory(PROXY).setPoolName("poolOne")
        .create("regionOne")).isNotNull();
    assertThat(cc.createClientRegionFactory(CACHING_PROXY).setPoolName("poolTwo")
        .create("regionTwo")).isNotNull();
    assertThatThrownBy(() -> cc.createClientRegionFactory(CACHING_PROXY)
        .setPoolName("nonExistingPool").create("regionThree"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("The connection pool nonExistingPool has not been created");
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
      cache = r.getCache();
      distSys = cache.getDistributedSystem();
      r.localDestroyRegion();
    }
  }

  public static class MyCacheListener extends CacheListenerAdapter<Object, Object> {
  }

  public static class MyCustomExpiry implements CustomExpiry<Object, Object> {
    @Override
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return null;
    }

    @Override
    public void close() {}
  }
}
