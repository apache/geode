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
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheTransactionManagerCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.ClientCacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.IgnoredException;

/**
 * Tests 6.5 cache.xml features.
 * 
 * @author gregp, skumar, darrel
 * @since 6.5
 */
public class CacheXml65DUnitTest extends CacheXml61DUnitTest {
  
  private final static String ALIAS1;

  private final static String ALIAS2;
  
  static {
    String tmp_alias1 = "localhost";
    String tmp_alias2 = "localhost";
//    try {
//      tmp_alias1 = getServerHostName(Host.getHost(0));
//      InetSocketAddress addr = createINSA(tmp_alias1, 10000);
//      tmp_alias2 = addr.getHostName();
//    }
//    catch (IllegalArgumentException suppress) {
//      // The runnables dont have a Host object initialized, but they dont need
//      // access to the aliases so its ok to suppress this.
//    }
//    finally {
      ALIAS1 = tmp_alias1;
      ALIAS2 = tmp_alias2;
//    }
  }
  
  private static InetSocketAddress createINSA(String host, int port) {
    try {
      InetAddress hostAddr = InetAddress.getByName(host);
      return new InetSocketAddress(hostAddr, port);
    }
    catch (UnknownHostException cause) {
      IllegalArgumentException ex = new IllegalArgumentException(
          "Unknown host " + host);
      ex.initCause(cause);
      throw ex;
    }
  }
  // ////// Constructors

  public CacheXml65DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_6_5;
  }
  /**
   * test for checking default value of PR_Single_Hop feature.
   * Test for checking default value of multiuser-authentication attribute.
   */
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
    assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT, cp
        .getFreeConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_LOAD_CONDITIONING_INTERVAL, cp
        .getLoadConditioningInterval());
    assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, cp
        .getSocketBufferSize());
    assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS, cp
        .getThreadLocalConnections());
    assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, cp.getReadTimeout());
    assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, cp.getMinConnections());
    assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, cp.getMaxConnections());
    assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, cp.getRetryAttempts());
    assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, cp.getIdleTimeout());
    assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, cp.getPingInterval());
    assertEquals(PoolFactory.DEFAULT_STATISTIC_INTERVAL, cp
        .getStatisticInterval());
    assertEquals(PoolFactory.DEFAULT_SERVER_GROUP, cp.getServerGroup());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED, cp
        .getSubscriptionEnabled());
    assertEquals(PoolFactory.DEFAULT_PR_SINGLE_HOP_ENABLED, cp
        .getPRSingleHopEnabled());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY, cp
        .getSubscriptionRedundancy());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT, cp
        .getSubscriptionMessageTrackingTimeout());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL, cp
        .getSubscriptionAckInterval());
    assertEquals(PoolFactory.DEFAULT_MULTIUSER_AUTHENTICATION, cp
        .getMultiuserAuthentication());
  }

  public void testDiskStore() throws CacheException {
    CacheCreation cache = new CacheCreation();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {new File("").getAbsoluteFile()};
    DiskStore ds1 = dsf.setAllowForceCompaction(true)
                      .setAutoCompact(true)
                      .setCompactionThreshold(100)
                      .setMaxOplogSize(2)
                      .setTimeInterval(10)
                      .setWriteBufferSize(15)
                      .setQueueSize(12)
                      .setDiskDirsAndSizes(dirs1, new int[] {1024*20})
                      .create(getUniqueName()+1);
    File[] dirs2 = new File[] {new File("").getAbsoluteFile()};
    DiskStore ds2 = dsf.setAllowForceCompaction(false)
    .setAutoCompact(false)
    .setCompactionThreshold(99)
    .setMaxOplogSize(1)
    .setTimeInterval(9)
    .setWriteBufferSize(14)
    .setQueueSize(11)
    .setDiskDirsAndSizes(dirs2, new int[] {1024*40})
    .create(getUniqueName()+2);
    
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    attrs.setDiskStoreName(getUniqueName()+1);
    attrs.setDiskSynchronous(true);
    RegionCreation root = (RegionCreation)
      cache.createRegion("root", attrs);
    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      attrs.setDiskStoreName(getUniqueName()+2);
      Region subwithdiskstore = root.createSubregion("subwithdiskstore", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      Region subwithdefaultdiskstore = root.createSubregion("subwithdefaultdiskstore", attrs);
    }

    testXml(cache);
  }

  /**
   * test for enabling PRsingleHop feature.
   * Test for enabling multiuser-authentication attribute.
   */
  public void testExplicitConnectionPool() throws CacheException {
    getSystem();
    CacheCreation cache = new CacheCreation();
    PoolFactory f = cache.createPoolFactory();
    f.addServer(ALIAS2, 3777).addServer(ALIAS1, 3888);
    f.setFreeConnectionTimeout(12345).setLoadConditioningInterval(12345)
        .setSocketBufferSize(12345).setThreadLocalConnections(true)
        .setPRSingleHopEnabled(true).setReadTimeout(12345).setMinConnections(
            12346).setMaxConnections(12347).setRetryAttempts(12348)
        .setIdleTimeout(12349)
        .setPingInterval(12350)
        .setStatisticInterval(12351)
        .setServerGroup("mygroup")
        // commented this out until queues are implemented
        // .setQueueEnabled(true)
        .setSubscriptionRedundancy(12345)
        .setSubscriptionMessageTrackingTimeout(12345)
        .setSubscriptionAckInterval(333)
        .setMultiuserAuthentication(true);
    f.create("mypool");
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    attrs.setDataPolicy(DataPolicy.EMPTY); // required for multiuser mode
    cache.createVMRegion("rootNORMAL", attrs);
    IgnoredException.addIgnoredException("Connection refused: connect");
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
    assertEquals(true, cp.getPRSingleHopEnabled());
    assertEquals(12345, cp.getReadTimeout());
    assertEquals(12346, cp.getMinConnections());
    assertEquals(12347, cp.getMaxConnections());
    assertEquals(12348, cp.getRetryAttempts());
    assertEquals(12349, cp.getIdleTimeout());
    assertEquals(12350, cp.getPingInterval());
    assertEquals(12351, cp.getStatisticInterval());
    assertEquals("mygroup", cp.getServerGroup());
    // commented this out until queues are implemented
    // assertEquals(true, cp.getQueueEnabled());
    assertEquals(12345, cp.getSubscriptionRedundancy());
    assertEquals(12345, cp.getSubscriptionMessageTrackingTimeout());
    assertEquals(333, cp.getSubscriptionAckInterval());
    assertEquals(true, cp.getMultiuserAuthentication());
  }
  
  public void testDiskStoreValidation() throws CacheException {
    CacheCreation cache = new CacheCreation();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    DiskStore ds1 = dsf.create(getUniqueName());
    
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    attrs.setDiskStoreName(getUniqueName());
    RegionCreation root;
    try {
      root = (RegionCreation)cache.createRegion("root", attrs);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(LocalizedStrings.DiskStore_IS_USED_IN_NONPERSISTENT_REGION.toLocalizedString()));
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }

    EvictionAttributes ea = EvictionAttributes.createLRUEntryAttributes(1000, EvictionAction.OVERFLOW_TO_DISK);
    attrs.setEvictionAttributes(ea);
    try {
      root = (RegionCreation)cache.createRegion("root", attrs);
    } catch (IllegalStateException e) {
      Assert.fail("With eviction of overflow to disk, region can specify disk store name", e);
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }
    
    File dir = new File("testDiskStoreValidation");
    dir.mkdir();
    dir.deleteOnExit();

    File[] dirs2 = new File[] { dir, new File("").getAbsoluteFile()};
    try {
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskDirs(dirs2); 
      factory.setDiskStoreName(getUniqueName());
      RegionAttributes ra = factory.create();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskDirs or setDiskWriteAttributes", getUniqueName()})));
    } catch (Exception ex) { 
      Assert.fail("Unexpected exception", ex);
    }

    try {
      AttributesFactory factory = new AttributesFactory();
      factory.setDiskStoreName(getUniqueName());
      factory.setDiskDirs(dirs2);
      RegionAttributes ra = factory.create();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskDirs", getUniqueName()})));
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }

    testXml(cache);
  }

  public void testDiskStoreFactory() throws CacheException {
    CacheCreation cache = new CacheCreation();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();

    DiskStore ds1;
    try {
      dsf.setDiskDirs(new File[] {new File("non_exist_dir")});
      ds1 = dsf.create(getUniqueName());
      //NOT required any more as we create the disk store directory during disk-store creation
      //fail("Expected IllegalStateException");
    } catch (IllegalArgumentException e) {
      // got expected exception
    }

    dsf.setDiskDirs(new File[] {new File(".")});
    ds1 = dsf.create(getUniqueName());
  
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    attrs.setDiskStoreName(getUniqueName());
    AttributesFactory factory = new AttributesFactory(attrs);
    RegionAttributes ra = factory.create();
    
    RegionCreation root;
    try {
      root = (RegionCreation)cache.createRegion("root", ra);
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }
    
    factory = new AttributesFactory();
    factory.setDiskStoreName(getUniqueName());
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    ra = factory.create();
    
    RegionCreation root2;
    try {
      root2 = (RegionCreation)cache.createRegion("root2", ra);
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }

    factory = new AttributesFactory();
    factory.setDiskStoreName(null);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    ra = factory.create();
    
    RegionCreation root3;
    try {
      root3 = (RegionCreation)cache.createRegion("root3", ra);
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }

    testXml(cache);
  }
  public void testRedefineOfDefaultDiskStore() throws CacheException {
    CacheCreation cache = new CacheCreation();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    DiskStore ds1 = dsf.create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
  
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    AttributesFactory factory = new AttributesFactory(attrs);
    RegionAttributes ra = factory.create();
    
    RegionCreation root;
    try {
      root = (RegionCreation)cache.createRegion("root", ra);
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    DiskStore ds2 = c.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    assertNotNull(ds2);
    assertEquals(ds1.getAutoCompact(), ds2.getAutoCompact());
  }

  /**
   * Make sure you can create a persistent partitioned region from xml.
   */
  public void testPersistentPartition() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    
    cache.createRegion("parRoot", attrs);
    
    Region r = cache.getRegion("parRoot");
    assertEquals(DataPolicy.PERSISTENT_PARTITION, r.getAttributes().getDataPolicy());
    
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    assertEquals(DataPolicy.PERSISTENT_PARTITION, region.getAttributes().getDataPolicy());
    // since CacheTestCase remoteTearDown does not destroy PartitionedRegion
    region.localDestroyRegion();
  }
  
  public void testBridgeAttributesRelatedToHAOverFlow65() throws CacheException {
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
    File overflowDirectory = new File("overFlow");
    overflowDirectory.mkdirs();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {overflowDirectory};
    DiskStore ds1 = dsf.setDiskDirs(dirs1).create(getUniqueName());
    csc.setDiskStoreName(getUniqueName());
    try {
      csc.setOverflowDirectory("overFlow");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(
          LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setOverflowDirectory", getUniqueName()})));
    } catch (Exception ex) {
      Assert.fail("Unexpected exception", ex);
    }

    cache.getLogger().config(
        "Eviction disk store : "
            + csc.getDiskStoreName());
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
    DiskStore dsi = cache.findDiskStore(chaqf.getDiskStoreName());
    assertEquals("overFlow", dsi.getDiskDirs()[0].toString());
  }
  
  public void testClientSubscriptionQueueUsingDefaultDS() throws CacheException {
    CacheCreation cache = new CacheCreation();
    cache.setMessageSyncInterval(3445);
    CacheServer bs = cache.addCacheServer();
    ClientSubscriptionConfig csc  =   bs.getClientSubscriptionConfig();
    csc.setEvictionPolicy("entry");
    cache.getLogger().config(
        "EvictionPolicy : " + csc.getEvictionPolicy());
    csc.setCapacity(501);
    // don't set diskstore or overflowdir
    cache.getLogger().config(
        "EvictionCapacity : " + csc.getCapacity());
    cache.getLogger().config(
        "Eviction disk store : "
            + csc.getDiskStoreName());
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
    File curDir = new File(".").getAbsoluteFile();
    File lockFile = new File(curDir, "DRLK_IF" + GemFireCacheImpl.DEFAULT_DS_NAME +".lk");
    assertTrue(lockFile.exists());
  }

  /**
   * Tests that a region created with a named attributes set programmatically
   * for delta propogation has the correct attributes.
   * 
   */
  public void testTransactionWriter() throws CacheException
  {
    CacheCreation creation = new CacheCreation();
    CacheTransactionManagerCreation ctmc = new CacheTransactionManagerCreation();
    ctmc.setWriter(new TestTransactionWriter());
    creation.addCacheTransactionManagerCreation(ctmc);
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    TransactionWriter tw = c.getCacheTransactionManager().getWriter();
    assertTrue("tw should be TransactionWriter, but it is:"+tw,tw instanceof TestTransactionWriter);
  }


  /**
   * Tests that a region created with a named attributes with diskstore
   */
  public void testDiskStoreInTemplates() throws CacheException
  {
    File dir = new File("west");
    dir.mkdir();
    dir.deleteOnExit();
    
    dir = new File("east");
    dir.mkdir();
    dir.deleteOnExit();

    setXmlFile(findFile("ewtest.xml"));

    String regionName_west = "orders/west";
    String regionName_east = "orders/east";

    Cache cache = getCache();
    
    // verify diskstores
    DiskStore ds = cache.findDiskStore("persistentDiskStore1");
    assertNotNull(ds);
    assertEquals(500, ds.getQueueSize());
    File[] dirs = ds.getDiskDirs();
    assertEquals("west", dirs[0].getPath());
    
    ds = cache.findDiskStore("persistentDiskStore2");
    assertNotNull(ds);
    assertEquals(500, ds.getQueueSize());
    dirs = ds.getDiskDirs();
    assertEquals("east", dirs[0].getPath());

    // verify templates
    assertNotNull(cache.getRegionAttributes("nack"));
    RegionAttributes attrs = cache.getRegionAttributes("persistent");
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, attrs.getDataPolicy());
    assertEquals(false, attrs.isDiskSynchronous());
    assertEquals("persistentDiskStore1", attrs.getDiskStoreName());

    Region region = cache.getRegion(regionName_west);
    assertNotNull(region);

    attrs = region.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, attrs.getDataPolicy());
    assertEquals(false, attrs.isDiskSynchronous());
    assertEquals("persistentDiskStore1", attrs.getDiskStoreName());

    region = cache.getRegion(regionName_east);
    assertNotNull(region);

    // Make sure that attributes can be "overridden"
    attrs = region.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, attrs.getDataPolicy());
    assertEquals(false, attrs.isDiskSynchronous());
    assertEquals("persistentDiskStore2", attrs.getDiskStoreName());
    
    // bug 41934
    String regionName_datap = "data-p";
    region = cache.getRegion(regionName_datap);
    assertNotNull(region);
    attrs = region.getAttributes();
    PartitionAttributes pa = attrs.getPartitionAttributes();
    assertEquals(1, pa.getRedundantCopies());
    assertEquals(3, pa.getTotalNumBuckets());
    assertEquals(DataPolicy.PERSISTENT_PARTITION, attrs.getDataPolicy());
  }
  
  public void testBackupFiles() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    File backup1 = new File("/back/me/up");
    File backup2 = new File("/me/too/please");
    cache.addBackup(backup1);
    cache.addBackup(backup2);
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(Arrays.asList(new File[] {backup1, backup2}), c.getBackupFiles());
  }

  public void testClientCache() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    cache.setCopyOnRead(true);
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.getCopyOnRead());
    assertEquals(true, c.isClient());
    for (ClientRegionShortcut pra: ClientRegionShortcut.values()) {
      assertNotNull(c.getRegionAttributes(pra.name()));
    }
    assertEquals(ClientRegionShortcut.values().length,
                 c.listRegionAttributes().size());
  }
  public void testNormalCache() throws CacheException {
    CacheCreation cache = new CacheCreation();
    cache.setCopyOnRead(true);
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.getCopyOnRead());
    assertEquals(false, c.isClient());
    for (RegionShortcut pra: RegionShortcut.values()) {
      assertNotNull(c.getRegionAttributes(pra.name()));
    }
    assertEquals(RegionShortcut.values().length,
                 c.listRegionAttributes().size());
  }
  public void testPARTITION() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("partition", "PARTITION");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partition");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
  }
  public void testPARTITION_REDUNDANT() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("rpartition", "PARTITION_REDUNDANT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartition");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }
  public void testPARTITION_PERSISTENT() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("ppartition", "PARTITION_PERSISTENT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("ppartition");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
  }
  public void testPARTITION_REDUNDANT_PERSISTENT() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("prpartition", "PARTITION_REDUNDANT_PERSISTENT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("prpartition");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }
  public void testPARTITION_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    ResourceManagerCreation rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(55.0f);
    rmc.setCriticalHeapPercentage(80.0f);
    cache.setResourceManagerCreation(rmc);
    RegionCreation root = (RegionCreation)
      cache.createRegion("partitionoverflow", "PARTITION_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partitionoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(55.0f,
                 c.getResourceManager().getEvictionHeapPercentage());
    assertEquals(80.0f,
                 c.getResourceManager().getCriticalHeapPercentage());
  }
  public void testPARTITION_REDUNDANT_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("rpartitionoverflow", "PARTITION_REDUNDANT_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartitionoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testPARTITION_PERSISTENT_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    ResourceManagerCreation rmc = new ResourceManagerCreation();
    rmc.setCriticalHeapPercentage(80.0f);
    cache.setResourceManagerCreation(rmc);
    RegionCreation root = (RegionCreation)
      cache.createRegion("ppartitionoverflow", "PARTITION_PERSISTENT_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("ppartitionoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(80.0f,
                 c.getResourceManager().getCriticalHeapPercentage());
    assertEquals(75.0f,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testPARTITION_REDUNDANT_PERSISTENT_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    ResourceManagerCreation rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(0.0f); // test bug 42130
    cache.setResourceManagerCreation(rmc);
    RegionCreation root = (RegionCreation)
      cache.createRegion("prpartitionoverflow", "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("prpartitionoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(0.0f,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testPARTITION_HEAP_LRU() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("partitionlru", "PARTITION_HEAP_LRU");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partitionlru");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testPARTITION_REDUNDANT_HEAP_LRU() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("rpartitionlru", "PARTITION_REDUNDANT_HEAP_LRU");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartitionlru");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }

  public void testREPLICATE() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("replicate", "REPLICATE");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicate");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }
  public void testREPLICATE_PERSISTENT() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("preplicate", "REPLICATE_PERSISTENT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("preplicate");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }
  public void testREPLICATE_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("replicateoverflow", "REPLICATE_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicateoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testREPLICATE_PERSISTENT_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("preplicateoverflow", "REPLICATE_PERSISTENT_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("preplicateoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testREPLICATE_HEAP_LRU() throws CacheException, IOException
  {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("replicatelru", "REPLICATE_HEAP_LRU");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicatelru");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
    assertEquals(new SubscriptionAttributes(InterestPolicy.ALL),
                 ra.getSubscriptionAttributes());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testLOCAL() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("local", "LOCAL");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("local");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
  }
  public void testLOCAL_PERSISTENT() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("plocal", "LOCAL_PERSISTENT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("plocal");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
  }
  public void testLOCAL_HEAP_LRU() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("locallru", "LOCAL_HEAP_LRU");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("locallru");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testLOCAL_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("localoverflow", "LOCAL_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("localoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testLOCAL_PERSISTENT_OVERFLOW() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("cpolocal", "LOCAL_PERSISTENT_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("cpolocal");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }

  public void testPARTITION_PROXY() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("partitionProxy", "PARTITION_PROXY");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partitionProxy");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
  }
  public void testPARTITION_PROXY_REDUNDANT() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("rpartitionProxy", "PARTITION_PROXY_REDUNDANT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartitionProxy");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
  }
  public void testREPLICATE_PROXY() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("replicateProxy", "REPLICATE_PROXY");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicateProxy");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  public void testPROXY() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("proxy", "PROXY");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("proxy");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
  }
  public void testCACHING_PROXY() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("cproxy", "CACHING_PROXY");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cproxy");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
  }
  public void testCACHING_PROXY_HEAP_LRU() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("cproxylru", "CACHING_PROXY_HEAP_LRU");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cproxylru");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testCACHING_PROXY_OVERFLOW() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("cproxyoverflow", "CACHING_PROXY_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cproxyoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testClientLOCAL() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("local", "LOCAL");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("local");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
  }
  public void testClientLOCAL_HEAP_LRU() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("locallru", "LOCAL_HEAP_LRU");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("locallru");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testClientLOCAL_OVERFLOW() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("localoverflow", "LOCAL_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("localoverflow");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }
  public void testClientLOCAL_PERSISTENT() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("cplocal", "LOCAL_PERSISTENT");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cplocal");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
  }
  public void testClientLOCAL_PERSISTENT_OVERFLOW() throws CacheException {
    ClientCacheCreation cache = new ClientCacheCreation();
    RegionCreation root = (RegionCreation)
      cache.createRegion("cpolocal", "LOCAL_PERSISTENT_OVERFLOW");
    testXml(cache);
    GemFireCacheImpl c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cpolocal");
    assertNotNull(r);
    RegionAttributes ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                 c.getResourceManager().getEvictionHeapPercentage());
  }

  // @todo add some tests to make sure the new smarter region-attribut defaults work from xml
}
