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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.LocalRegion.NonTXEntry;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.WanTest;

/**
 * @since GemFire 7.0.1
 */
@Category({WanTest.class})
public class UpdateVersionDUnitTest extends JUnit4DistributedTestCase {

  protected static final String regionName = "testRegion";
  protected static Cache cache;
  private static Set<IgnoredException> expectedExceptions = new HashSet<IgnoredException>();

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    Invoke.invokeInEveryVM(this::closeCache);
  }

  @Test
  public void testUpdateVersionAfterCreateWithSerialSender() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // locator site1
    VM vm1 = host.getVM(1); // server2 site1

    VM vm2 = host.getVM(2); // locator site2
    VM vm3 = host.getVM(3); // server1 site2
    VM vm4 = host.getVM(4); // server2 site2

    final String key = "key-1";

    // Site 1
    Integer lnPort = (Integer) vm0.invoke(() -> this.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> this.createCache(lnPort));
    vm1.invoke(() -> this.createSender("ln1", 2, false, 10, 1, false, false, null, true));

    vm1.invoke(() -> this.createPartitionedRegion(regionName, "ln1", 1, 1));
    vm1.invoke(() -> this.startSender("ln1"));
    vm1.invoke(() -> this.waitForSenderRunningState("ln1"));

    // Site 2
    Integer nyPort = (Integer) vm2.invoke(() -> this.createFirstRemoteLocator(2, lnPort));
    Integer nyRecPort = (Integer) vm3.invoke(() -> this.createReceiver(nyPort));

    vm3.invoke(() -> this.createPartitionedRegion(regionName, "", 1, 1));
    vm4.invoke(() -> this.createCache(nyPort));
    vm4.invoke(() -> this.createPartitionedRegion(regionName, "", 1, 1));

    VersionTag localTag = vm1.invoke(() -> putEntryAndGetPartitionedRegionVersionTag(key));
    VersionTag remoteTag = vm4.invoke(() -> getPartitionedRegionVersionTag(key, localTag));

    assertEquals("Local and remote site have different timestamps", localTag.getVersionTimeStamp(),
        remoteTag.getVersionTimeStamp());
  }

  @Test
  public void testUpdateVersionAfterCreateWithSerialSenderOnDR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // locator site1
    VM vm1 = host.getVM(1); // server1 site1

    VM vm2 = host.getVM(2); // locator site2
    VM vm3 = host.getVM(3); // server1 site2
    VM vm4 = host.getVM(4); // server2 site2

    final String key = "key-1";

    // Site 1
    Integer lnPort = (Integer) vm0.invoke(() -> this.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> this.createCache(lnPort));
    vm1.invoke(() -> this.createSender("ln1", 2, false, 10, 1, false, false, null, true));

    vm1.invoke(() -> this.createReplicatedRegion(regionName, "ln1"));
    vm1.invoke(() -> this.startSender("ln1"));
    vm1.invoke(() -> this.waitForSenderRunningState("ln1"));

    // Site 2
    Integer nyPort = (Integer) vm2.invoke(() -> this.createFirstRemoteLocator(2, lnPort));
    Integer nyRecPort = (Integer) vm3.invoke(() -> this.createReceiver(nyPort));

    vm3.invoke(() -> this.createReplicatedRegion(regionName, ""));
    vm4.invoke(() -> this.createCache(nyPort));
    vm4.invoke(() -> this.createReplicatedRegion(regionName, ""));

    VersionTag localTag = vm1.invoke(() -> putEntryAndGetReplicatedRegionVersionTag(key));
    VersionTag remoteTag = vm4.invoke(() -> getReplicatedRegionVersionTag(key, localTag));

    assertEquals("Local and remote site have different timestamps", localTag.getVersionTimeStamp(),
        remoteTag.getVersionTimeStamp());
  }

  @Test
  public void testUpdateVersionAfterCreateWithParallelSender() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // locator site1
    VM vm1 = host.getVM(1); // server1 site1

    VM vm2 = host.getVM(2); // locator site2
    VM vm3 = host.getVM(3); // server1 site2
    VM vm4 = host.getVM(4); // server2 site2

    // Site 1
    Integer lnPort = vm0.invoke(() -> this.createFirstLocatorWithDSId(1));

    final String key = "key-1";

    vm1.invoke(() -> this.createCache(lnPort));
    vm1.invoke(() -> this.createSender("ln1", 2, true, 10, 1, false, false, null, true));

    vm1.invoke(() -> this.createPartitionedRegion(regionName, "ln1", 1, 1));
    vm1.invoke(() -> this.startSender("ln1"));
    vm1.invoke(() -> this.waitForSenderRunningState("ln1"));

    // Site 2
    Integer nyPort = (Integer) vm2.invoke(() -> this.createFirstRemoteLocator(2, lnPort));
    Integer nyRecPort = (Integer) vm3.invoke(() -> this.createReceiver(nyPort));

    vm3.invoke(() -> this.createPartitionedRegion(regionName, "", 1, 1));
    vm4.invoke(() -> this.createCache(nyPort));
    vm4.invoke(() -> this.createPartitionedRegion(regionName, "", 1, 1));

    VersionTag localTag = vm1.invoke(() -> putEntryAndGetPartitionedRegionVersionTag(key));
    VersionTag remoteTag = vm4.invoke(() -> getPartitionedRegionVersionTag(key, localTag));

    assertEquals("Local and remote site have different timestamps", localTag.getVersionTimeStamp(),
        remoteTag.getVersionTimeStamp());
  }

  @Test
  public void testUpdateVersionAfterCreateWithConcurrentSerialSender() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // locator site1
    VM vm1 = host.getVM(1); // server1 site1

    VM vm2 = host.getVM(2); // locator site2
    VM vm3 = host.getVM(3); // server1 site2
    VM vm4 = host.getVM(4); // server2 site2

    // Site 1
    Integer lnPort = (Integer) vm0.invoke(() -> this.createFirstLocatorWithDSId(1));

    final String key = "key-1";

    vm1.invoke(() -> this.createCache(lnPort));
    vm1.invoke(
        () -> this.createConcurrentSender("ln1", 2, false, 10, 2, false, false, null, true, 2));

    vm1.invoke(() -> this.createPartitionedRegion(regionName, "ln1", 1, 1));
    vm1.invoke(() -> this.startSender("ln1"));
    vm1.invoke(() -> this.waitForSenderRunningState("ln1"));

    // Site 2
    Integer nyPort = (Integer) vm2.invoke(() -> this.createFirstRemoteLocator(2, lnPort));
    Integer nyRecPort = (Integer) vm3.invoke(() -> this.createReceiver(nyPort));

    vm3.invoke(() -> this.createPartitionedRegion(regionName, "", 1, 1));
    vm4.invoke(() -> this.createCache(nyPort));
    vm4.invoke(() -> this.createPartitionedRegion(regionName, "", 1, 1));

    VersionTag localTag = vm1.invoke(() -> putEntryAndGetPartitionedRegionVersionTag(key));
    VersionTag remoteTag = vm4.invoke(() -> getPartitionedRegionVersionTag(key, localTag));

    assertEquals("Local and remote site have different timestamps", localTag.getVersionTimeStamp(),
        remoteTag.getVersionTimeStamp());
  }

  private VersionTag putEntryAndGetReplicatedRegionVersionTag(String key) {
    Region region = cache.getRegion(regionName);
    assertTrue(region instanceof DistributedRegion);

    region.put(key, "value-1");
    region.put(key, "value-2");
    Entry entry = region.getEntry(key);
    assertTrue(entry instanceof NonTXEntry);
    RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

    VersionStamp stamp = regionEntry.getVersionStamp();

    // Create a duplicate entry version tag from stamp with newer
    // time-stamp.
    VersionSource memberId = (VersionSource) cache.getDistributedSystem().getDistributedMember();
    VersionTag versionTag = VersionTag.create(memberId);

    int entryVersion = stamp.getEntryVersion() - 1;
    int dsid = stamp.getDistributedSystemId();

    // Increment the time by 1 in case the time is the same as the previous event.
    // The entry's version timestamp can be incremented by 1 in certain circumstances.
    // See AbstractRegionEntry.generateVersionTag.
    long time = System.currentTimeMillis() + 1;

    versionTag.setEntryVersion(entryVersion);
    versionTag.setDistributedSystemId(dsid);
    versionTag.setVersionTimeStamp(time);
    versionTag.setIsRemoteForTesting();

    EntryEventImpl event =
        createNewEvent((DistributedRegion) region, versionTag, entry.getKey(), "value-3");

    ((LocalRegion) region).basicUpdate(event, false, true, 0L, false);

    // Verify the new stamp
    entry = region.getEntry(key);
    assertTrue(entry instanceof NonTXEntry);
    regionEntry = ((NonTXEntry) entry).getRegionEntry();

    stamp = regionEntry.getVersionStamp();
    assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion", time,
        stamp.getVersionTimeStamp());
    assertEquals(entryVersion + 1, stamp.getEntryVersion());
    assertEquals(dsid, stamp.getDistributedSystemId());

    return stamp.asVersionTag();
  }

  private VersionTag putEntryAndGetPartitionedRegionVersionTag(String key) {
    Region region = cache.getRegion(regionName);
    assertTrue(region instanceof PartitionedRegion);

    region.put(key, "value-1");
    region.put(key, "value-2");
    Entry entry = region.getEntry(key);
    assertTrue(entry instanceof EntrySnapshot);
    RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

    VersionStamp stamp = regionEntry.getVersionStamp();

    // Create a duplicate entry version tag from stamp with newer
    // time-stamp.
    VersionSource memberId = (VersionSource) cache.getDistributedSystem().getDistributedMember();
    VersionTag versionTag = VersionTag.create(memberId);

    int entryVersion = stamp.getEntryVersion() - 1;
    int dsid = stamp.getDistributedSystemId();

    // Increment the time by 1 in case the time is the same as the previous event.
    // The entry's version timestamp can be incremented by 1 in certain circumstances.
    // See AbstractRegionEntry.generateVersionTag.
    long time = System.currentTimeMillis() + 1;

    versionTag.setEntryVersion(entryVersion);
    versionTag.setDistributedSystemId(dsid);
    versionTag.setVersionTimeStamp(time);
    versionTag.setIsRemoteForTesting();

    EntryEventImpl event =
        createNewEvent((PartitionedRegion) region, versionTag, entry.getKey(), "value-3");

    ((LocalRegion) region).basicUpdate(event, false, true, 0L, false);

    // Verify the new stamp
    entry = region.getEntry(key);
    assertTrue(entry instanceof EntrySnapshot);
    regionEntry = ((EntrySnapshot) entry).getRegionEntry();

    stamp = regionEntry.getVersionStamp();
    assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion", time,
        stamp.getVersionTimeStamp());
    assertEquals(++entryVersion, stamp.getEntryVersion());
    assertEquals(dsid, stamp.getDistributedSystemId());

    return stamp.asVersionTag();
  }

  private VersionTag getReplicatedRegionVersionTag(final String key, final VersionTag localTag) {
    final Region region = cache.getRegion(regionName);

    await().until(() -> region.getEntry(key) != null);

    await().until(() -> {
      Entry entry = region.getEntry(key);
      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();
      return regionEntry.getVersionStamp().getVersionTimeStamp() == localTag.getVersionTimeStamp();
    });

    Entry entry = region.getEntry(key);
    assertTrue(entry instanceof NonTXEntry);
    RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

    VersionStamp stamp = regionEntry.getVersionStamp();

    return stamp.asVersionTag();
  }

  private VersionTag getPartitionedRegionVersionTag(final String key, final VersionTag localTag) {
    final PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

    await().until(() -> {
      Entry<?, ?> entry = null;
      try {
        entry = region.getDataStore().getEntryLocally(0, key, false, false);
      } catch (EntryNotFoundException | ForceReattemptException e) {
        // expected
      } catch (PRLocallyDestroyedException e) {
        throw new RuntimeException("unexpected exception", e);
      }
      if (entry != null) {
        LogWriterUtils.getLogWriter().info("found entry " + entry);
      }
      return (entry != null);
    });

    await().until(() -> {
      Entry entry = region.getEntry(key);
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();
      return regionEntry.getVersionStamp().getVersionTimeStamp() == localTag.getVersionTimeStamp();
    });

    Entry entry = region.getEntry(key);
    assertTrue(entry instanceof EntrySnapshot);
    RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

    VersionStamp stamp = regionEntry.getVersionStamp();

    return stamp.asVersionTag();
  }

  private VersionTagHolder createNewEvent(LocalRegion region, VersionTag tag, Object key,
      Object value) {
    VersionTagHolder updateEvent = new VersionTagHolder(tag);
    updateEvent.setOperation(Operation.UPDATE);
    updateEvent.setRegion(region);
    if (region instanceof PartitionedRegion) {
      updateEvent.setKeyInfo(((PartitionedRegion) region).getKeyInfo(key));
    } else {
      updateEvent.setKeyInfo(new KeyInfo(key, value, null));
    }
    updateEvent.setNewValue(value);
    updateEvent.setGenerateCallbacks(true);
    updateEvent.distributedMember = region.getSystem().getDistributedMember();
    updateEvent.setNewEventId(region.getSystem());
    return updateEvent;
  }

  /*
   * Helper Methods
   */

  private void createCache(Integer locPort) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    props.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    IgnoredException ex =
        new IgnoredException("could not get remote locator information for remote site");
    cache.getLogger().info(ex.getAddMessage());
    expectedExceptions.add(ex);
    ex = new IgnoredException("Pool ln1 is not available");
    cache.getLogger().info(ex.getAddMessage());
    expectedExceptions.add(ex);
  }

  private void closeCache() {
    if (cache != null && !cache.isClosed()) {
      for (IgnoredException expectedException : expectedExceptions) {
        cache.getLogger().info(expectedException.getRemoveMessage());
      }
      expectedExceptions.clear();
      cache.getDistributedSystem().disconnect();
      cache.close();
    }
    cache = null;
  }

  public void createSender(String dsName, int remoteDsId, boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent, GatewayEventFilter filter,
      boolean isManualStart) {
    File persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};
    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  private void createPartitionedRegion(String regionName, String senderIds, Integer redundantCopies,
      Integer totalNumBuckets) {
    AttributesFactory fact = new AttributesFactory();
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pFact = new PartitionAttributesFactory();
    pFact.setTotalNumBuckets(totalNumBuckets);
    pFact.setRedundantCopies(redundantCopies);
    pFact.setRecoveryDelay(0);
    fact.setPartitionAttributes(pFact.create());
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }

  private void createReplicatedRegion(String regionName, String senderIds) {
    AttributesFactory fact = new AttributesFactory();
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    fact.setDataPolicy(DataPolicy.REPLICATE);
    fact.setScope(Scope.DISTRIBUTED_ACK);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }

  private void waitForSenderRunningState(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    await()
        .until(() -> sender != null && sender.isRunning());
  }

  private Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + port + "]");
    props.setProperty(START_LOCATOR,
        "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    props.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    startLocatorDistributedSystem(props);
    return port;
  }

  private void startLocatorDistributedSystem(Properties props) {
    // Start start the locator with a LOCATOR_DM_TYPE and not a NORMAL_DM_TYPE
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    try {
      getSystem(props);
    } finally {
      System.clearProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE);
    }
  }

  private void createConcurrentSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart, int concurrencyLevel) {
    File persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};

    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setDispatcherThreads(concurrencyLevel);
      gateway.create(dsName, remoteDsId);
    }
  }

  private int createReceiver(int locPort) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  private void startSender(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    for (GatewaySender sender : senders) {
      if (sender.getId().equals(senderId)) {
        sender.start();
        break;
      }
    }
  }

  protected static class MyLocatorCallback extends LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();

    private final Set removedLocators = new HashSet();

    @Override
    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    @Override
    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    public boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }

    public boolean waitForRemove(InetSocketAddress locator, long time) throws InterruptedException {
      return waitFor(removedLocators, locator, time);
    }

    private synchronized boolean waitFor(Set set, InetSocketAddress locator, long time)
        throws InterruptedException {
      long remaining = time;
      long endTime = System.currentTimeMillis() + time;
      while (!set.contains(locator) && remaining >= 0) {
        wait(remaining);
        remaining = endTime - System.currentTimeMillis();
      }
      return set.contains(locator);
    }

    public synchronized Set getDiscovered() {
      return new HashSet(discoveredLocators);
    }

    public synchronized Set getRemoved() {
      return new HashSet(removedLocators);
    }
  }

  private Integer createFirstLocatorWithDSId(int dsId) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + port + "]");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(START_LOCATOR,
        "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    startLocatorDistributedSystem(props);
    return port;
  }
}
