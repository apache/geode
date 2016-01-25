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
package com.gemstone.gemfire.cache30;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbeAdapter;
import com.gemstone.gemfire.cache.server.ServerMetrics;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

import junit.framework.Assert;

/**
 * Tests 5.7 cache.xml features.
 * 
 * @author darrel
 * @since 5.7
 */

public class CacheXml57DUnitTest extends CacheXml55DUnitTest
{
  //
  private final static String ALIAS1;
  private final static String ALIAS2;

  static {
    String tmp_alias1 = "localhost";
    String tmp_alias2 = "localhost";
//    try {
//      tmp_alias1 = getServerHostName(Host.getHost(0)); 
//      InetSocketAddress addr = createINSA(tmp_alias1, 10000);
//      tmp_alias2 = addr.getHostName();
//    } catch (IllegalArgumentException suppress) {
//      //The runnables dont have a Host object initialized, but they dont need 
//      //access to the aliases so its ok to suppress this.
//    } finally {
      ALIAS1 = tmp_alias1;
      ALIAS2 = tmp_alias2;
//    }
  }

  // ////// Constructors

  public CacheXml57DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_5_7;
  }

  /**
   * Tests the groups subelement on bridge-server.
   */
  public void testDefaultCacheServerGroups() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheServer bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    bs.setGroups(CacheServer.DEFAULT_GROUPS);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(CacheServer.DEFAULT_GROUPS, server.getGroups());
  }
  public void testOneCacheServerGroups() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheServer bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    String[] groups = new String[]{"group1"};
    bs.setGroups(groups);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(Arrays.asList(groups), Arrays.asList(server.getGroups()));
  }
  public void testTwoCacheServerGroups() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheServer bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    String[] groups = new String[]{"group1", "group2"};
    bs.setGroups(groups);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(Arrays.asList(groups), Arrays.asList(server.getGroups()));
  }
  public void testDefaultCacheServerBindAddress() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheServer bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(CacheServer.DEFAULT_BIND_ADDRESS, server.getBindAddress());
  }
  public void testCacheServerBindAddress() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheServer bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    final String BA = ALIAS1;
    bs.setBindAddress(BA);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(BA, server.getBindAddress());
  }
  public void testCacheServerHostnameForClients() throws CacheException {
    CacheCreation cache = new CacheCreation();
    CacheServer bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    final String BA = ALIAS1;
    bs.setBindAddress(BA);
    bs.setHostnameForClients("clientHostName");
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(BA, server.getBindAddress());
    assertEquals("clientHostName", server.getHostnameForClients());
  }
  public void testExplicitConnectionPool() throws CacheException {
    getSystem();
    CacheCreation cache = new CacheCreation();
    PoolFactory f = cache.createPoolFactory();
    f.addServer(ALIAS2, 3777).addServer(ALIAS1, 3888);
    f.setFreeConnectionTimeout(12345)
      .setLoadConditioningInterval(12345)
      .setSocketBufferSize(12345)
      .setThreadLocalConnections(true)
      .setReadTimeout(12345)
      .setMinConnections(12346)
      .setMaxConnections(12347)
      .setRetryAttempts(12348)
      .setIdleTimeout(12349)
      .setPingInterval(12350)
      .setStatisticInterval(12351)
      .setServerGroup("mygroup")
      // commented this out until queues are implemented
//       .setQueueEnabled(true)
      .setSubscriptionRedundancy(12345)
      .setSubscriptionMessageTrackingTimeout(12345)
      .setSubscriptionAckInterval(333);
    f.create("mypool");
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    addExpectedException("Connection refused: connect");
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Region r = c.getRegion("rootNORMAL");
    assertNotNull(r);
    assertEquals("mypool", r.getAttributes().getPoolName());
    Pool cp = PoolManager.find("mypool");
    assertNotNull(cp);
    assertEquals(0, cp.getLocators().size());
    assertEquals(2, cp.getServers().size());
    assertEquals(createINSA(ALIAS2, 3777), cp.getServers().get(0));
    assertEquals(createINSA(ALIAS1, 3888), cp.getServers().get(1));
    assertEquals(12345, cp.getFreeConnectionTimeout());
    assertEquals(12345, cp.getLoadConditioningInterval());
    assertEquals(12345, cp.getSocketBufferSize());
    assertEquals(true, cp.getThreadLocalConnections());
    assertEquals(12345, cp.getReadTimeout());
    assertEquals(12346, cp.getMinConnections());
    assertEquals(12347, cp.getMaxConnections());
    assertEquals(12348, cp.getRetryAttempts());
    assertEquals(12349, cp.getIdleTimeout());
    assertEquals(12350, cp.getPingInterval());
    assertEquals(12351, cp.getStatisticInterval());
    assertEquals("mygroup", cp.getServerGroup());
      // commented this out until queues are implemented
    //    assertEquals(true, cp.getQueueEnabled());
    assertEquals(12345, cp.getSubscriptionRedundancy());
    assertEquals(12345, cp.getSubscriptionMessageTrackingTimeout());
    assertEquals(333, cp.getSubscriptionAckInterval());
  }
  public void testDefaultConnectionPool() throws CacheException {
    getSystem();
    CacheCreation cache = new CacheCreation();
    PoolFactory f = cache.createPoolFactory();
    f.addLocator(ALIAS2, 3777);
    f.create("mypool");
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Region r = c.getRegion("rootNORMAL");
    assertNotNull(r);
    assertEquals("mypool", r.getAttributes().getPoolName());
    Pool cp = PoolManager.find("mypool");
    assertNotNull(cp);
    assertEquals(1, cp.getLocators().size());
    assertEquals(0, cp.getServers().size());
    assertEquals(createINSA(ALIAS2, 3777), cp.getLocators().get(0));
    assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT, cp.getFreeConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_LOAD_CONDITIONING_INTERVAL, cp.getLoadConditioningInterval());
    assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, cp.getSocketBufferSize());
    assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS, cp.getThreadLocalConnections());
    assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, cp.getReadTimeout());
    assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, cp.getMinConnections());
    assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, cp.getMaxConnections());
    assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, cp.getRetryAttempts());
    assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, cp.getIdleTimeout());
    assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, cp.getPingInterval());
    assertEquals(PoolFactory.DEFAULT_STATISTIC_INTERVAL, cp.getStatisticInterval());
    assertEquals(PoolFactory.DEFAULT_SERVER_GROUP, cp.getServerGroup());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED, cp.getSubscriptionEnabled());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY, cp.getSubscriptionRedundancy());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT, cp.getSubscriptionMessageTrackingTimeout());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL, cp.getSubscriptionAckInterval());
  }
  public void testTwoConnectionPools() throws CacheException {
    getSystem();
    CacheCreation cache = new CacheCreation();
    PoolFactory f = cache.createPoolFactory();
    f.addLocator(ALIAS2, 3777).create("mypool");
    f.reset().addLocator(ALIAS1, 3888).create("mypool2");
    try {
      f.create("mypool");
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    try {
      f.create("mypool2");
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Region r = c.getRegion("rootNORMAL");
    assertNotNull(r);
    assertEquals("mypool", r.getAttributes().getPoolName());
    Pool cp = PoolManager.find("mypool");
    assertNotNull(cp);
    assertEquals(0, cp.getServers().size());
    assertEquals(1, cp.getLocators().size());
    assertEquals(createINSA(ALIAS2, 3777), cp.getLocators().get(0));
    cp = PoolManager.find("mypool2");
    assertNotNull(cp);
    assertEquals(0, cp.getServers().size());
    assertEquals(1, cp.getLocators().size());
    assertEquals(createINSA(ALIAS1, 3888), cp.getLocators().get(0));
  }
  private static InetSocketAddress createINSA(String host, int port) {
    try {
      InetAddress hostAddr = InetAddress.getByName(host);
      return new InetSocketAddress(hostAddr, port);
    } catch (UnknownHostException cause) {
      IllegalArgumentException ex = new IllegalArgumentException("Unknown host " + host);
      ex.initCause(cause);
      throw ex;
    }
  }
  public void testNoConnectionPools() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    ExpectedException expectedException = CacheTestCase.addExpectedException(LocalizedStrings.AbstractRegion_THE_CONNECTION_POOL_0_HAS_NOT_BEEN_CREATED.toLocalizedString("mypool"));
    try {
      testXml(cache);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    } finally {
      expectedException.remove();
    }
  }
  public void testAlreadyExistingPool() throws CacheException {
    getSystem();
    PoolFactoryImpl f = (PoolFactoryImpl)
      PoolManager.createFactory();
    f.setStartDisabled(true).addLocator(ALIAS2, 12345).create("mypool");
    try {
      // now make sure declarative cache can't create the same pool
      CacheCreation cache = new CacheCreation();
      cache.createPoolFactory().addLocator(ALIAS2, 12345).create("mypool");
      ExpectedException expectedException = CacheTestCase.addExpectedException(LocalizedStrings.PoolManagerImpl_POOL_NAMED_0_ALREADY_EXISTS.toLocalizedString("mypool"));
      try {
        testXml(cache);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      } finally {
        expectedException.remove();
      }
    } finally {
      PoolManager.close();
    }
  }

  public void testDynamicRegionFactoryConnectionPool() throws CacheException, IOException {
    addExpectedException("Connection reset");
    addExpectedException("SocketTimeoutException");
    addExpectedException("ServerConnectivityException");
    addExpectedException("Socket Closed");
    getSystem();
    VM vm0 = Host.getHost(0).getVM(0);
    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    vm0.invoke(new SerializableCallable("Create cache server") {
      public Object call() throws IOException {
        DynamicRegionFactory.get().open();
        Cache cache = getCache();
        CacheServer bridge = cache.addCacheServer();
        bridge.setPort(port);
        bridge.setNotifyBySubscription(true);
        bridge.start();
        return null;
      }
    });
    CacheCreation cache = new CacheCreation();
    cache.createPoolFactory()
    .addServer(getServerHostName(vm0.getHost()), port)
      .setSubscriptionEnabled(true)
    .create("connectionPool");
    cache.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(null, "connectionPool", false, false));
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(false, DynamicRegionFactory.get().getConfig().getRegisterInterest());
    assertEquals(false, DynamicRegionFactory.get().getConfig().getPersistBackup());
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(null, DynamicRegionFactory.get().getConfig().getDiskDir());
    assertEquals("connectionPool", DynamicRegionFactory.get().getConfig().getPoolName());
    Region dr = getCache().getRegion("__DynamicRegions");    
    if(dr != null) {
      dr.localDestroyRegion();      
    }
  }

  /**
   * Tests the client subscription attributes (<code>eviction-policy</code>,
   * <code>capacity</code> and <code>overflow-directory</code>) related to
   * client subscription config in gemfire cache-server framework
   *
   * @throws CacheException
   * @since 5.7
   */
  public void testBridgeAttributesRelatedToHAOverFlow() throws CacheException {
    CacheCreation cache = new CacheCreation();
    cache.setMessageSyncInterval(3445);
    CacheServer bs = cache.addCacheServer();
    ClientSubscriptionConfig csc  =   bs.getClientSubscriptionConfig();
    csc.setEvictionPolicy("entry");
    cache.getLogger().config(
        "EvictionPolicy : " + csc.getEvictionPolicy());
    csc.setCapacity(501);
    cache.getLogger().config(
        "EvictionCapacity : " + csc.getCapacity());
    csc.setOverflowDirectory("overFlow");
    cache.getLogger().config(
        "EvictionOverflowDirectory : "
            + csc.getOverflowDirectory());
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    CacheServer server = (CacheServer)cache.getCacheServers().iterator()
        .next();
    assertNotNull(server);
    ClientSubscriptionConfig chaqf = server.getClientSubscriptionConfig();
    assertEquals("entry", chaqf.getEvictionPolicy());
    assertEquals(501, chaqf.getCapacity());
    assertEquals("overFlow", chaqf.getOverflowDirectory());
  }
  
  public void testBridgeLoadProbe() {
    CacheCreation cache = new CacheCreation();
    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    server.setLoadProbe(new MyLoadProbe());
    
    testXml(cache);
    
    Cache c= getCache();
    server = c.getCacheServers().get(0);
    Assert.assertEquals(MyLoadProbe.class,server.getLoadProbe().getClass());
  }
  
  public void testLoadPollInterval() {
    CacheCreation cache = new CacheCreation();
    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    server.setLoadPollInterval(12345);
    
    testXml(cache);
    
    Cache c = getCache();
    server = c.getCacheServers().get(0);
    Assert.assertEquals(12345, server.getLoadPollInterval());
  }
  
  public static class MyLoadProbe extends ServerLoadProbeAdapter implements Declarable {
    public ServerLoad getLoad(ServerMetrics metrics) {
      return null;
    }

    public void init(Properties props) {
    }
    
    public boolean equals(Object o) {
      return o instanceof MyLoadProbe;
    }
  }
  
  static public class Expiry1 implements CustomExpiry, Declarable{
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    public void init(Properties props) {
    }

    public void close() {
    } 
  }

  static public class Expiry2 implements CustomExpiry, Declarable {
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    public void init(Properties props) {
    }

    public void close() {
    } 
  }
  
  static public class Expiry3 implements CustomExpiry, Declarable {
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    public void init(Properties props) {
    }

    public void close() {
    } 
  }
  
  static public class Expiry4 implements CustomExpiry, Declarable {
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    public void init(Properties props) {
    }

    public void close() {
    } 
  }
  
  static public class Expiry5 implements CustomExpiry, Declarable2 {
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    public void init(Properties props) {
    }

    public void close() {
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.cache.xmlcache.Declarable2#getConfig()
     */
    public Properties getConfig() {
      Properties p = new Properties();
      p.put("prop1", "val1");
      p.put("prop2", "val2");
      return p;
    } 
  }
  
  /**
   * Test both customEntryIdleTime and customEntryTimeToLife
   */
  public void testCustomEntryXml() {
    CacheCreation cache = new CacheCreation();

    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);

    RegionCreation root =
      (RegionCreation) cache.createRegion("root", attrs);

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
      attrs.setInitialCapacity(142);
      attrs.setLoadFactor(42.42f);
      attrs.setStatisticsEnabled(true);
      attrs.setCustomEntryIdleTimeout(new Expiry1());
      attrs.setCustomEntryTimeToLive(new Expiry5());

      root.createSubregion("one", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setInitialCapacity(242);
      attrs.setStatisticsEnabled(true);
      attrs.setCustomEntryIdleTimeout(new Expiry2());

      Region region = root.createSubregion("two", attrs);

      {
        attrs = new RegionAttributesCreation(cache);
        attrs.setScope(Scope.DISTRIBUTED_ACK);
        attrs.setLoadFactor(43.43f);
        attrs.setStatisticsEnabled(true);
        attrs.setCustomEntryIdleTimeout(new Expiry3());
        attrs.setCustomEntryTimeToLive(new Expiry4());

        region.createSubregion("three", attrs);
      }
    }

    testXml(cache);
  }

  public void testPreloadDataPolicy() throws CacheException {
    CacheCreation cache = new CacheCreation();

    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      cache.createRegion("rootNORMAL", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootNORMAL_ALL", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setMirrorType(MirrorType.KEYS_VALUES);
      cache.createRegion("rootREPLICATE", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      cache.createRegion("rootPERSISTENT_REPLICATE", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      cache.createRegion("rootEMPTY", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootEMPTY_ALL", attrs);
    }
    {
      RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.PRELOADED);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootPRELOADED_ALL", attrs);
    }

    testXml(cache);
  
  }

  /**
   * Test EnableSubscriptionConflation region attribute
   * @since 5.7
   */
  public void testEnableSubscriptionConflationAttribute() throws CacheException {

    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setEnableSubscriptionConflation(true);
    cache.createRegion("root", attrs);
    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().getEnableSubscriptionConflation());
  }
}
