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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionStamp;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.InternalGatewaySenderFactory;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * @author Shobhit Agarwal
 * @since 7.0.1
 */
public class UpdateVersionDUnitTest extends DistributedTestCase {

  protected static final String regionName = "testRegion";
  protected static Cache cache;
  private static Set<IgnoredException>expectedExceptions = new HashSet<IgnoredException>();

  
  
  public UpdateVersionDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void preTearDown() throws Exception {
    closeCache();
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() {
      closeCache();
     } });
  }
  
  public void testUpdateVersionAfterCreateWithSerialSender() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // server1 site1
    VM vm1 = host.getVM(1); // server2 site1

    VM vm2 = host.getVM(2); // server1 site2
    VM vm3 = host.getVM(3); // server2 site2

    final String key = "key-1";

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(UpdateVersionDUnitTest.class, "createFirstLocatorWithDSId", new Object[] { 1 });

    vm0.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { lnPort});
    vm0.invoke(UpdateVersionDUnitTest.class, "createSender", new Object[] { "ln1", 2, false, 10, 1, false, false, null, true });
    
    vm0.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "ln1", 1, 1});
    vm0.invoke(UpdateVersionDUnitTest.class, "startSender", new Object[] { "ln1" });
    vm0.invoke(UpdateVersionDUnitTest.class, "waitForSenderRunningState", new Object[] { "ln1" });

    //Site 2
    Integer nyPort = (Integer)vm2.invoke(UpdateVersionDUnitTest.class, "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm2.invoke(UpdateVersionDUnitTest.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "", 1, 1});
    vm3.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { nyPort});
    vm3.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "", 1, 1});    
    
    final VersionTag tag = (VersionTag) vm0.invoke(new SerializableCallable("Update a single entry and get its version") {
      
      @Override
      public Object call() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        assertTrue(region instanceof PartitionedRegion);

        region.put(key, "value-1");
        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        // Create a duplicate entry version tag from stamp with newer
        // time-stamp.
        VersionSource memberId = (VersionSource) cache.getDistributedSystem().getDistributedMember();
        VersionTag tag = VersionTag.create(memberId);

        int entryVersion = stamp.getEntryVersion()-1;
        int dsid = stamp.getDistributedSystemId();
        long time = System.currentTimeMillis();

        tag.setEntryVersion(entryVersion);
        tag.setDistributedSystemId(dsid);
        tag.setVersionTimeStamp(time);
        tag.setIsRemoteForTesting();

        EntryEventImpl event = createNewEvent((PartitionedRegion) region, tag,
            entry.getKey(), "value-2");

        ((LocalRegion) region).basicUpdate(event, false, true, 0L, false);

        // Verify the new stamp
        entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        stamp = regionEntry.getVersionStamp();
        assertEquals(
            "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
            time, stamp.getVersionTimeStamp());
        assertEquals(++entryVersion, stamp.getEntryVersion());
        assertEquals(dsid, stamp.getDistributedSystemId());

        return stamp.asVersionTag();
      }
    });

    VersionTag remoteTag = (VersionTag) vm3.invoke(new SerializableCallable("Get timestamp from remote site") {
      
      @Override
      public Object call() throws Exception {
        
        Cache cache = CacheFactory.getAnyInstance();
        final PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);

        // wait for entry to be received
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            Entry<?,?> entry = null;
            try {
              entry = region.getDataStore().getEntryLocally(0, key, false, false, false);
            } catch (EntryNotFoundException e) {
              // expected
            } catch (ForceReattemptException e) {
              // expected
            } catch (PRLocallyDestroyedException e) {
              throw new RuntimeException("unexpected exception", e);
            }
            if (entry != null) {
              LogWriterUtils.getLogWriter().info("found entry " + entry);
            }
            return (entry != null);
          }

          public String description() {
            return "Expected "+key+" to be received on remote WAN site";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        wc = new WaitCriterion() {
          public boolean done() {
            Entry entry = region.getEntry(key);
            assertTrue(entry instanceof EntrySnapshot);
            RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();
            return regionEntry.getVersionStamp().getVersionTimeStamp() == tag.getVersionTimeStamp();
          }
          public String description() {
            return "waiting for timestamp to be updated";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        Entry entry = region.getEntry(key);
        assertTrue("entry class is wrong: " + entry, entry instanceof EntrySnapshot);
        RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        return stamp.asVersionTag();
      }
    });

    assertEquals("Local and remote site have different timestamps", tag.getVersionTimeStamp(), remoteTag.getVersionTimeStamp());
  }

  public void testUpdateVersionAfterCreateWithSerialSenderOnDR() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // server1 site1
    VM vm1 = host.getVM(1); // server2 site1

    VM vm2 = host.getVM(2); // server1 site2
    VM vm3 = host.getVM(3); // server2 site2

    final String key = "key-1";

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(UpdateVersionDUnitTest.class, "createFirstLocatorWithDSId", new Object[] { 1 });

    vm0.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { lnPort});
    vm0.invoke(UpdateVersionDUnitTest.class, "createSender", new Object[] { "ln1", 2, false, 10, 1, false, false, null, true });
    
    vm0.invoke(UpdateVersionDUnitTest.class, "createReplicatedRegion", new Object[] {regionName, "ln1"});
    vm0.invoke(UpdateVersionDUnitTest.class, "startSender", new Object[] { "ln1" });
    vm0.invoke(UpdateVersionDUnitTest.class, "waitForSenderRunningState", new Object[] { "ln1" });

    //Site 2
    Integer nyPort = (Integer)vm2.invoke(UpdateVersionDUnitTest.class, "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm2.invoke(UpdateVersionDUnitTest.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(UpdateVersionDUnitTest.class, "createReplicatedRegion", new Object[] {regionName, ""});
    vm3.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { nyPort });
    vm3.invoke(UpdateVersionDUnitTest.class, "createReplicatedRegion", new Object[] {regionName, ""});    
    
    final VersionTag tag = (VersionTag) vm0.invoke(new SerializableCallable("Update a single entry and get its version") {
      
      @Override
      public Object call() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        assertTrue(region instanceof DistributedRegion);

        region.put(key, "value-1");
        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof NonTXEntry);
        RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        // Create a duplicate entry version tag from stamp with newer
        // time-stamp.
        VersionSource memberId = (VersionSource) cache.getDistributedSystem().getDistributedMember();
        VersionTag tag = VersionTag.create(memberId);

        int entryVersion = stamp.getEntryVersion()-1;
        int dsid = stamp.getDistributedSystemId();
        long time = System.currentTimeMillis();

        tag.setEntryVersion(entryVersion);
        tag.setDistributedSystemId(dsid);
        tag.setVersionTimeStamp(time);
        tag.setIsRemoteForTesting();

        EntryEventImpl event = createNewEvent((DistributedRegion) region, tag,
            entry.getKey(), "value-2");

        ((LocalRegion) region).basicUpdate(event, false, true, 0L, false);

        // Verify the new stamp
        entry = region.getEntry(key);
        assertTrue(entry instanceof NonTXEntry);
        regionEntry = ((NonTXEntry) entry).getRegionEntry();

        stamp = regionEntry.getVersionStamp();
        assertEquals(
            "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
            time, stamp.getVersionTimeStamp());
        assertEquals(entryVersion+1, stamp.getEntryVersion());
        assertEquals(dsid, stamp.getDistributedSystemId());

        return stamp.asVersionTag();
      }
    });

    VersionTag remoteTag = (VersionTag) vm3.invoke(new SerializableCallable("Get timestamp from remote site") {
      
      @Override
      public Object call() throws Exception {
        
        Cache cache = CacheFactory.getAnyInstance();
        final Region region = cache.getRegion(regionName);

        // wait for entry to be received
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return (region.getEntry(key) != null);
          }

          public String description() {
            return "Expected key-1 to be received on remote WAN site";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        wc = new WaitCriterion() {
          public boolean done() {
            Entry entry = region.getEntry(key);
            assertTrue(entry instanceof NonTXEntry);
            RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();
            return regionEntry.getVersionStamp().getVersionTimeStamp() == tag.getVersionTimeStamp();
          }
          public String description() {
            return "waiting for timestamp to be updated";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof NonTXEntry);
        RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        return stamp.asVersionTag();
      }
    });

    assertEquals("Local and remote site have different timestamps", tag.getVersionTimeStamp(), remoteTag.getVersionTimeStamp());
  }

  public void testUpdateVersionAfterCreateWithParallelSender() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // server1 site1
    VM vm1 = host.getVM(1); // server2 site1

    VM vm2 = host.getVM(2); // server1 site2
    VM vm3 = host.getVM(3); // server2 site2

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(UpdateVersionDUnitTest.class, "createFirstLocatorWithDSId", new Object[] { 1 });

    final String key = "key-1";

    vm0.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { lnPort});
    vm0.invoke(UpdateVersionDUnitTest.class, "createSender", new Object[] { "ln1", 2, true, 10, 1, false, false, null, true });
    
    vm0.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "ln1", 1, 1});
    vm0.invoke(UpdateVersionDUnitTest.class, "startSender", new Object[] { "ln1" });
    vm0.invoke(UpdateVersionDUnitTest.class, "waitForSenderRunningState", new Object[] { "ln1" });
    
    //Site 2
    Integer nyPort = (Integer)vm2.invoke(UpdateVersionDUnitTest.class, "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm2.invoke(UpdateVersionDUnitTest.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "", 1, 1});

    vm3.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { nyPort});
    vm3.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "", 1, 1});
    
    final VersionTag tag = (VersionTag) vm0.invoke(new SerializableCallable("Put a single entry and get its version") {
      
      @Override
      public Object call() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        assertTrue(region instanceof PartitionedRegion);

        region.put(key, "value-1");
        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        // Create a duplicate entry version tag from stamp with newer
        // time-stamp.
        VersionSource memberId = (VersionSource) cache.getDistributedSystem().getDistributedMember();
        VersionTag tag = VersionTag.create(memberId);

        int entryVersion = stamp.getEntryVersion()-1;
        int dsid = stamp.getDistributedSystemId();
        long time = System.currentTimeMillis();

        tag.setEntryVersion(entryVersion);
        tag.setDistributedSystemId(dsid);
        tag.setVersionTimeStamp(time);
        tag.setIsRemoteForTesting();

        EntryEventImpl event = createNewEvent((PartitionedRegion) region, tag,
            entry.getKey(), "value-2");

        ((LocalRegion) region).basicUpdate(event, false, true, 0L, false);

        // Verify the new stamp
        entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        stamp = regionEntry.getVersionStamp();
        assertEquals(
            "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
            time, stamp.getVersionTimeStamp());
        assertEquals(++entryVersion, stamp.getEntryVersion());
        assertEquals(dsid, stamp.getDistributedSystemId());

        return stamp.asVersionTag();
      }
    });

    VersionTag remoteTag = (VersionTag) vm3.invoke(new SerializableCallable("Get timestamp from remote site") {
      
      @Override
      public Object call() throws Exception {
        
        Cache cache = CacheFactory.getAnyInstance();
        final PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);

        // wait for entry to be received
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            Entry<?,?> entry = null;
            try {
              entry = region.getDataStore().getEntryLocally(0, key, false, false, false);
            } catch (EntryNotFoundException e) {
              // expected
            } catch (ForceReattemptException e) {
              // expected
            } catch (PRLocallyDestroyedException e) {
              throw new RuntimeException("unexpected exception", e);
            }
            if (entry != null) {
              LogWriterUtils.getLogWriter().info("found entry " + entry);
            }
            return (entry != null);
          }

          public String description() {
            return "Expected key-1 to be received on remote WAN site";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        wc = new WaitCriterion() {
          public boolean done() {
            Entry entry = region.getEntry(key);
            assertTrue(entry instanceof EntrySnapshot);
            RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();
            return regionEntry.getVersionStamp().getVersionTimeStamp() == tag.getVersionTimeStamp();
          }
          public String description() {
            return "waiting for timestamp to be updated";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        return stamp.asVersionTag();
      }
    });

    assertEquals("Local and remote site have different timestamps", tag.getVersionTimeStamp(), remoteTag.getVersionTimeStamp());
  }

  public void testUpdateVersionAfterCreateWithConcurrentSerialSender() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // server1 site1
    VM vm1 = host.getVM(1); // server2 site1

    VM vm2 = host.getVM(2); // server1 site2
    VM vm3 = host.getVM(3); // server2 site2

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(UpdateVersionDUnitTest.class, "createFirstLocatorWithDSId", new Object[] { 1 });

    final String key = "key-1";

    vm0.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { lnPort });
    vm0.invoke(UpdateVersionDUnitTest.class, "createConcurrentSender", new Object[] { "ln1", 2, false, 10, 2, false, false, null, true, 2 });
    
    vm0.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "ln1", 1, 1});
    vm0.invoke(UpdateVersionDUnitTest.class, "startSender", new Object[] { "ln1" });
    vm0.invoke(UpdateVersionDUnitTest.class, "waitForSenderRunningState", new Object[] { "ln1" });
    
    //Site 2
    Integer nyPort = (Integer)vm2.invoke(UpdateVersionDUnitTest.class, "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm2.invoke(UpdateVersionDUnitTest.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "", 1, 1});

    vm3.invoke(UpdateVersionDUnitTest.class, "createCache", new Object[] { nyPort });
    vm3.invoke(UpdateVersionDUnitTest.class, "createPartitionedRegion", new Object[] {regionName, "", 1, 1});    
    
    final VersionTag tag = (VersionTag) vm0.invoke(new SerializableCallable("Put a single entry and get its version") {
      
      @Override
      public Object call() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        assertTrue(region instanceof PartitionedRegion);

        region.put(key, "value-1");
        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        // Create a duplicate entry version tag from stamp with newer
        // time-stamp.
        VersionSource memberId = (VersionSource) cache.getDistributedSystem().getDistributedMember();
        VersionTag tag = VersionTag.create(memberId);

        int entryVersion = stamp.getEntryVersion()-1;
        int dsid = stamp.getDistributedSystemId();
        long time = System.currentTimeMillis();

        tag.setEntryVersion(entryVersion);
        tag.setDistributedSystemId(dsid);
        tag.setVersionTimeStamp(time);
        tag.setIsRemoteForTesting();

        EntryEventImpl event = createNewEvent((PartitionedRegion) region, tag,
            entry.getKey(), "value-2");

        ((LocalRegion) region).basicUpdate(event, false, true, 0L, false);

        // Verify the new stamp
        entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        stamp = regionEntry.getVersionStamp();
        assertEquals(
            "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
            time, stamp.getVersionTimeStamp());
        assertEquals(++entryVersion, stamp.getEntryVersion());
        assertEquals(dsid, stamp.getDistributedSystemId());

        return stamp.asVersionTag();
      }
    });

    VersionTag remoteTag = (VersionTag) vm3.invoke(new SerializableCallable("Get timestamp from remote site") {
      
      @Override
      public Object call() throws Exception {
        
        Cache cache = CacheFactory.getAnyInstance();
        final PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);

        // wait for entry to be received
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            Entry<?,?> entry = null;
            try {
              entry = region.getDataStore().getEntryLocally(0, key, false, false, false);
            } catch (EntryNotFoundException e) {
              // expected
            } catch (ForceReattemptException e) {
              // expected
            } catch (PRLocallyDestroyedException e) {
              throw new RuntimeException("unexpected exception", e);
            }
            if (entry != null) {
              LogWriterUtils.getLogWriter().info("found entry " + entry);
            }
            return (entry != null);
          }

          public String description() {
            return "Expected key-1 to be received on remote WAN site";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        wc = new WaitCriterion() {
          public boolean done() {
            Entry entry = region.getEntry(key);
            assertTrue(entry instanceof EntrySnapshot);
            RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();
            return regionEntry.getVersionStamp().getVersionTimeStamp() == tag.getVersionTimeStamp();
          }
          public String description() {
            return "waiting for timestamp to be updated";
          }
        };
        Wait.waitForCriterion(wc, 30000, 500, true);

        Entry entry = region.getEntry(key);
        assertTrue(entry instanceof EntrySnapshot);
        RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

        VersionStamp stamp = regionEntry.getVersionStamp();

        return stamp.asVersionTag();
      }
    });

    assertEquals("Local and remote site have different timestamps", tag.getVersionTimeStamp(), remoteTag.getVersionTimeStamp());
  }
  
  
  private EntryEventImpl createNewEvent(LocalRegion region, VersionTag tag, Object key, Object value) {
    EntryEventImpl updateEvent = EntryEventImpl.createVersionTagHolder(tag);
    updateEvent.setOperation(Operation.UPDATE);
    updateEvent.setRegion(region);
    if (region instanceof PartitionedRegion) {
      updateEvent.setKeyInfo(((PartitionedRegion)region).getKeyInfo(key));
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

  private static void createCache(Integer locPort) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest(getTestMethodName());
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    props.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds); 
    IgnoredException ex = new IgnoredException("could not get remote locator information for remote site");
    cache.getLogger().info(ex.getAddMessage());
    expectedExceptions.add(ex);
    ex = new IgnoredException("Pool ln1 is not available");
    cache.getLogger().info(ex.getAddMessage());
    expectedExceptions.add(ex);
  }
  
  private static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      for (IgnoredException expectedException: expectedExceptions) {
        cache.getLogger().info(expectedException.getRemoveMessage());
      }
      expectedExceptions.clear();
      cache.getDistributedSystem().disconnect();
      cache.close();
    }
    cache = null;
  }

  public static void createSender(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize,
      boolean isConflation, boolean isPersistent, GatewayEventFilter filter,
      boolean isManualStart) {
    File persistentDirectory = new File(dsName + "_disk_"
        + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] { persistentDirectory };
    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
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
      ((InternalGatewaySenderFactory) gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  
  public static void createPartitionedRegion(String regionName, String senderIds, Integer redundantCopies, Integer totalNumBuckets){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    pfact.setRecoveryDelay(0);
    fact.setPartitionAttributes(pfact.create());
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }

  public static void createReplicatedRegion(String regionName, String senderIds){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    fact.setDataPolicy(DataPolicy.REPLICATE);
    fact.setScope(Scope.DISTRIBUTED_ACK);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }

  public static void waitForSenderRunningState(String senderId){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    final GatewaySender sender = getGatewaySenderById(senders, senderId);
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (sender != null && sender.isRunning()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected sender isRunning state to be true but is false";
      }
    };
    Wait.waitForCriterion(wc, 300000, 500, true);
  }

  public static Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest(getTestMethodName());
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    props.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    test.getSystem(props);
    return port;
  }

  public static void createConcurrentSender(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManulaStart, int concurrencyLevel) {
    File persistentDirectory = new File(dsName +"_disk_"+System.currentTimeMillis()+"_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File [] dirs1 = new File[] {persistentDirectory};
    
    if(isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      ((InternalGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if(isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);
      
    }else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      ((InternalGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if(isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setDispatcherThreads(concurrencyLevel);
      gateway.create(dsName, remoteDsId);
    }
  }

  public static int createReceiver(int locPort) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest(getTestMethodName());
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");

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
      fail("Test " + test.getName() + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }

  public static void startSender(String senderId){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    sender.start();
  }

  protected static class MyLocatorCallback extends
      LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();

    private final Set removedLocators = new HashSet();

    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    public boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }

    public boolean waitForRemove(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(removedLocators, locator, time);
    }

    private synchronized boolean waitFor(Set set, InetSocketAddress locator,
        long time) throws InterruptedException {
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

  private static GatewaySender getGatewaySenderById(Set<GatewaySender> senders, String senderId) {
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        return s;
      }
    }
    //if none of the senders matches with the supplied senderid, return null
    return null;
  }

  public static Integer createFirstLocatorWithDSId(int dsId) {
    UpdateVersionDUnitTest test = new UpdateVersionDUnitTest(getTestMethodName());
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    props.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
    return port;
  }
}