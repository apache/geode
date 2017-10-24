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
/**
 * 
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.entries.VMThinDiskRegionEntryHeapObjectKey;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests that if a node doing GII experiences DiskAccessException, it should also not try to recover
 * from the disk
 */
@Category(DistributedTest.class)
public class Bug39079DUnitTest extends JUnit4CacheTestCase {

  private static final String REGION_NAME_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase =
      "IGNORE_EXCEPTION_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase";
  private static final String REGION_NAME_testGIIDiskAccessException =
      "IGNORE_EXCEPTION_testGIIDiskAccessException";

  private VM vm0;
  private VM vm1;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    vm0.invoke(() -> ignorePreAllocate(true));
    vm1.invoke(() -> ignorePreAllocate(true));
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();

    vm0.invoke(() -> ignorePreAllocate(false));
    vm1.invoke(() -> ignorePreAllocate(false));
  }

  /**
   * If the node experiences disk access exception during GII, it should get destroyed & not attempt
   * to recover from the disk
   */
  @Test
  public void testGIIDiskAccessException() throws Exception {
    vm0.invoke(createCacheForVM0());
    vm1.invoke(createCacheForVM1());

    // Create DiskRegion locally in controller VM also
    getSystem();


    assertTrue(getCache() != null);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(getDiskDirs())
        .create(getClass().getSimpleName()).getName());
    RegionAttributes attr = factory.create();
    Region region = getCache().createRegion(REGION_NAME_testGIIDiskAccessException, attr);

    // Now put entries in the disk region
    for (int i = 0; i < 100; ++i) {
      region.put(new Integer(i), new Integer(i));
    }

    // Now close the region in the controller VM
    region.close();

    // Now recreate the region but set the factory such that disk region entry object
    // used is customized by us to throw exception while writing to disk

    DistributedRegion distRegion =
        new DistributedRegion(REGION_NAME_testGIIDiskAccessException, attr, null,
            (GemFireCacheImpl) getCache(), new InternalRegionArguments().setDestroyLockFlag(true)
                .setRecreateFlag(false).setSnapshotInputStream(null).setImageTarget(null));

    distRegion.entries.setEntryFactory(TestAbstractDiskRegionEntry.getEntryFactory());
    region = null;

    try {
      region =
          ((GemFireCacheImpl) getCache()).createVMRegion(REGION_NAME_testGIIDiskAccessException,
              attr, new InternalRegionArguments().setInternalMetaRegion(distRegion)
                  .setDestroyLockFlag(true).setSnapshotInputStream(null).setImageTarget(null));
      fail("Expected DiskAccessException");
    } catch (DiskAccessException expected) {
    }

    assertTrue(region == null || region.isDestroyed()); // TODO: why is this an OR instead of
                                                        // deterministic?
  }

  /**
   * If IOException occurs while updating an entry in an already initialized DiskRegion ,then the
   * bridge servers should be stopped , if any running
   */
  @Test
  public void testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase() throws Exception {
    // create server cache
    Integer port = vm0.invoke(() -> createServerCache());

    // create cache client
    vm1.invoke(() -> createClientCache(NetworkUtils.getServerHostName(vm0.getHost()), port));

    // validate
    vm0.invoke(() -> validateRunningBridgeServerList());

    // close server cache
    vm0.invoke(() -> closeCacheAndDisconnect());

    // close client cache
    vm1.invoke(() -> closeCacheAndDisconnect());
  }

  private int createServerCache() throws IOException {
    createCache(new Properties());
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(REGION_NAME_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase);
    props.setOverflow(true);
    props.setRolling(true);
    props.setDiskDirs(getDiskDirs());
    props.setPersistBackup(true);

    Region region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(getCache(), props, Scope.DISTRIBUTED_ACK);
    assertNotNull(region);
    CacheServer bs1 = getCache().addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    bs1.setPort(port);
    bs1.start();
    return bs1.getPort();
  }

  private void closeCacheAndDisconnect() {
    closeCache();
    disconnectFromDS();
  }

  private void createCache(Properties props) {
    getSystem(props);
    assertNotNull(getCache());
  }

  private void validateRunningBridgeServerList() throws IOException {
    Region region = getCache()
        .getRegion(REGION_NAME_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase);
    try {
      region.create("key1", new byte[16]);
      region.create("key2", new byte[16]);

      // Get the oplog handle & hence the underlying file & close it
      UninterruptibleFileChannel oplogFileChannel =
          ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();

      try {
        oplogFileChannel.close();
        region.put("key2", new byte[16]);
        fail("Expected DiskAccessException");
      } catch (DiskAccessException expected) {
      }

      ((LocalRegion) region).getDiskStore().waitForClose();
      assertTrue(region.getRegionService().isClosed());

      region = null;
      List bsRunning = getCache().getCacheServers();
      assertTrue(bsRunning.isEmpty());
    } finally {
      if (region != null) {
        region.destroyRegion();
      }
    }
  }

  private void createClientCache(String host, Integer port1) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    createCache(props);

    PoolImpl pool = (PoolImpl) PoolManager.createFactory().addServer(host, port1.intValue())
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setThreadLocalConnections(true)
        .setMinConnections(0).setReadTimeout(20000).setRetryAttempts(1)
        .create(getClass().getSimpleName());

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(pool.getName());

    RegionAttributes attrs = factory.create();
    Region region = getCache().createRegion(
        REGION_NAME_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase, attrs);
    region.registerInterest("ALL_KEYS");
  }

  /**
   * This method is used to create Cache in VM0
   */
  private CacheSerializableRunnable createCacheForVM0() {
    return new CacheSerializableRunnable("createCache") {
      @Override
      public void run2() {
        try {
          getSystem();
          assertNotNull(getCache());

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(getDiskDirs())
              .create(getClass().getSimpleName()).getName());

          RegionAttributes attr = factory.create();
          getCache().createRegion(REGION_NAME_testGIIDiskAccessException, attr);
        } catch (Exception ex) {
          fail("Error Creating cache / region ", ex);
        }
      }
    };
  }

  /**
   * This method is used to create Cache in VM1
   */
  private CacheSerializableRunnable createCacheForVM1() {
    return new CacheSerializableRunnable("createCache") {
      @Override
      public void run2() {
        try {
          getSystem();
          assertNotNull(getCache());

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(getDiskDirs())
              .create(getClass().getSimpleName()).getName());

          RegionAttributes attr = factory.create();
          getCache().createRegion(REGION_NAME_testGIIDiskAccessException, attr);
        } catch (Exception ex) {
          fail("Error Creating cache / region ", ex);
        }
      }
    };
  }

  private void ignorePreAllocate(boolean flag) throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = flag;
  }

  private static class TestAbstractDiskRegionEntry extends VMThinDiskRegionEntryHeapObjectKey {

    protected TestAbstractDiskRegionEntry(RegionEntryContext r, Object key, Object value) {
      super(r, key, value);
    }

    private static RegionEntryFactory factory = new RegionEntryFactory() {

      @Override
      public RegionEntry createEntry(RegionEntryContext r, Object key, Object value) {
        throw new DiskAccessException(new IOException("Test Exception"));
      }

      @Override
      public Class getEntryClass() {
        return getClass();
      }

      @Override
      public RegionEntryFactory makeVersioned() {
        return this;
      }

      @Override
      public RegionEntryFactory makeOnHeap() {
        return this;
      }
    };

    /**
     * Overridden setValue method to throw exception
     */
    @Override
    protected void setValueField(Object v) {
      throw new DiskAccessException(new IOException("Test Exception"));
    }

    public static RegionEntryFactory getEntryFactory() {
      return factory;
    }
  }
}
