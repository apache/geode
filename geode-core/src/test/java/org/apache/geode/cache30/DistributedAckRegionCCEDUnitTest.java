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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetMember;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.UpdateOperation;
import org.apache.geode.internal.cache.VersionTagHolder;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.vmotion.VMotionObserver;
import org.apache.geode.internal.cache.vmotion.VMotionObserverHolder;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class DistributedAckRegionCCEDUnitTest extends DistributedAckRegionDUnitTest {

  @Override
  protected boolean supportsTransactions() {
    return true;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(CONSERVE_SOCKETS, "false");
    if (distributedSystemID > 0) {
      p.put(DISTRIBUTED_SYSTEM_ID, "" + distributedSystemID);
    }
    return p;
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type + " has been removed.");
    }
    AttributesFactory factory = new AttributesFactory(ra);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  @Ignore("replicates don't allow local destroy")
  @Override
  @Test
  public void testLocalDestroy() throws InterruptedException {
    // replicates don't allow local destroy
  }

  @Ignore("replicates don't allow local destroy")
  @Override
  @Test
  public void testEntryTtlLocalDestroy() throws InterruptedException {
    // replicates don't allow local destroy
  }

  /**
   * This test creates a server cache in vm0 and a peer cache in vm1. It then tests to see if GII
   * transferred tombstones to vm1 like it's supposed to. A client cache is created in vm2 and the
   * same sort of check is performed for register-interest.
   */
  @Test
  public void testGIISendsTombstones() throws Exception {
    versionTestGIISendsTombstones();
  }

  /**
   * test for bug #45564. a create() is received by region creator and then a later destroy() is
   * received in initial image and while the version info from the destroy is recorded we keep the
   * value from the create event
   */
  @Test
  public void testConcurrentOpWithGII() {
    if (this.getClass() != DistributedAckRegionCCEDUnitTest.class) {
      return; // not really a scope-related thing
    }
    final String name = this.getUniqueName() + "-CC";
    final String key = "mykey";
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    // create some destroyed entries so the GC service is populated
    SerializableCallable create = new SerializableCallable("create region") {
      public Object call() {
        RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
        CCRegion = (LocalRegion) f.create(name);
        return CCRegion.getDistributionManager().getDistributionManagerId();
      }
    };
    // do conflicting update() and destroy() on the region. We want the update() to
    // be sent with a message and the destroy() to be transferred in the initial image
    // and be the value that we want to keep
    InternalDistributedMember vm1ID = (InternalDistributedMember) vm1.invoke(create);

    AsyncInvocation partialCreate =
        vm2.invokeAsync(new SerializableCallable("create region with stall") {
          public Object call() throws Exception {
            final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
            RegionFactory f = cache.createRegionFactory(getRegionAttributes());
            InitialImageOperation.VMOTION_DURING_GII = true;
            // this will stall region creation at the point of asking for an initial image
            VMotionObserverHolder.setInstance(new VMotionObserver() {
              @Override
              public void vMotionBeforeCQRegistration() {}

              @Override
              public void vMotionBeforeRegisterInterest() {}

              @Override
              public void vMotionDuringGII(Set recipientSet, LocalRegion region) {
                InitialImageOperation.VMOTION_DURING_GII = false;
                int oldLevel =
                    LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
                LocalRegion ccregion = cache.getRegionByPath("/" + name);
                try {
                  // wait for the update op (sent below from vm1) to arrive, then allow the GII to
                  // happen
                  while (!ccregion.isDestroyed() && ccregion.getRegionEntry(key) == null) {
                    try {
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                      return;
                    }
                  }
                } finally {
                  LocalRegion.setThreadInitLevelRequirement(oldLevel);
                }
              }
            });
            try {
              CCRegion = (LocalRegion) f.create(name);
              // at this point we should have received the update op and then the GII, which should
              // overwrite
              // the conflicting update op
              assertFalse("expected initial image transfer to destroy entry",
                  CCRegion.containsKey(key));
            } finally {
              InitialImageOperation.VMOTION_DURING_GII = false;
            }
            return null;
          }
        });
    vm1.invoke(new SerializableRunnable("create conflicting events") {
      public void run() {
        // wait for the other to come on line
        long waitEnd = System.currentTimeMillis() + 45000;
        DistributionAdvisor adv = ((DistributedRegion) CCRegion).getCacheDistributionAdvisor();
        while (System.currentTimeMillis() < waitEnd && adv.adviseGeneric().isEmpty()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return;
          }
        }
        if (adv.adviseGeneric().isEmpty()) {
          fail("other member never came on line");
        }
        DistributedCacheOperation.LOSS_SIMULATION_RATIO = 200.0; // inhibit all messaging
        try {
          CCRegion.put("mykey", "initialValue");
          CCRegion.destroy("mykey");
        } finally {
          DistributedCacheOperation.LOSS_SIMULATION_RATIO = 0.0;
        }

        // generate a fake version tag for the message
        VersionTag tag = CCRegion.getRegionEntry(key).getVersionStamp().asVersionTag();
        // create a fake member ID that will be < mine and lose a concurrency check
        NetMember nm = CCRegion.getDistributionManager().getDistributionManagerId().getNetMember();
        InternalDistributedMember mbr = null;
        try {
          mbr = new InternalDistributedMember(nm.getInetAddress().getCanonicalHostName(),
              nm.getPort() - 1, "fake_id", "fake_id_ustring", DistributionManager.NORMAL_DM_TYPE,
              null, null);
          tag.setMemberID(mbr);
        } catch (UnknownHostException e) {
          org.apache.geode.test.dunit.Assert.fail("could not create member id", e);
        }

        // generate an event to distribute that contains the fake version tag
        EntryEventImpl event =
            EntryEventImpl.create(CCRegion, Operation.UPDATE, key, false, mbr, true, false);
        event.setNewValue("newValue");
        event.setVersionTag(tag);

        // this should update the controller's cache with the updated value but leave this cache
        // alone
        DistributedCacheOperation op = new UpdateOperation(event, tag.getVersionTimeStamp());
        op.distribute();
        event.release();
      }
    });
    try {
      partialCreate.getResult();
    } catch (Throwable e) {
      org.apache.geode.test.dunit.Assert.fail("async invocation in vm2 failed", e);
    }
  }

  protected void do_version_recovery_if_necessary(final VM vm0, final VM vm1, final VM vm2,
      final Object[] params) {
    // do nothing here
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEvents() throws Exception {
    versionTestConcurrentEvents();
  }

  @Test
  public void testClearWithConcurrentEvents() throws Exception {
    z_versionTestClearWithConcurrentEvents(true);
  }

  @Test
  public void testClearWithConcurrentEventsAsync() throws Exception {
    versionTestClearWithConcurrentEventsAsync();
  }

  @Ignore("Enable after fix for bug GEODE-1891 was #45704")
  @Test
  public void testClearOnNonReplicateWithConcurrentEvents() throws Exception {
    versionTestClearOnNonReplicateWithConcurrentEvents();
  }

  @Test
  public void testTombstones() {
    versionTestTombstones();
  }

  /**
   * make sure that an operation performed on a new region entry created after a tombstone has been
   * reaped is accepted by another member that has yet to reap the tombstone
   */
  @Test
  public void testTombstoneExpirationRace() {
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);
    // VM vm2 = Host.getHost(0).getVM(2);

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
          CCRegion = (LocalRegion) f.create(name);
          CCRegion.put("cckey0", "ccvalue");
          CCRegion.put("cckey0", "ccvalue"); // version number will end up at 4
        } catch (CacheException ex) {
          org.apache.geode.test.dunit.Assert.fail("While creating region", ex);
        }
      }
    };
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    // vm2.invoke(createRegion);
    vm1.invoke(new SerializableRunnable("Create local tombstone and adjust time") {
      public void run() {
        // make the entry for cckey0 a tombstone in this VM and set its modification time to be
        // older
        // than the tombstone GC interval. This means it could be in the process of being reaped by
        // distributed-GC
        RegionEntry entry = CCRegion.getRegionEntry("cckey0");
        VersionTag tag = entry.getVersionStamp().asVersionTag();
        assertTrue(tag.getEntryVersion() > 1);
        tag.setVersionTimeStamp(
            System.currentTimeMillis() - TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT - 1000);
        entry.getVersionStamp().setVersionTimeStamp(tag.getVersionTimeStamp());
        try {
          entry.makeTombstone(CCRegion, tag);
        } catch (RegionClearedException e) {
          org.apache.geode.test.dunit.Assert
              .fail("region was mysteriously cleared during unit testing", e);
        }
      }
    });
    // now remove the entry on vm0, simulating that it initiated a GC, and perform a CREATE with a
    // new version number
    vm0.invoke(new SerializableRunnable(
        "Locally destroy the entry and do a create that will be propagated with v1") {
      public void run() {
        CCRegion.getRegionMap().removeEntry("cckey0", CCRegion.getRegionEntry("cckey0"), true);
        if (CCRegion.getRegionEntry("ckey0") != null) {
          fail("expected removEntry to remove the entry from the region's map");
        }
        CCRegion.put("cckey0", "updateAfterReap");
      }
    });
    vm1.invoke(new SerializableRunnable("Check that the create() was applied") {
      public void run() {
        RegionEntry entry = CCRegion.getRegionEntry("cckey0");
        assertTrue(entry.getVersionStamp().getEntryVersion() == 1);
      }
    });
    disconnectAllFromDS();
  }

  /**
   * Test for bug #46087 and #46089 where the waiting thread pool is flooded with threads performing
   * distributed-GC. This could be moved to a JUnit test class.
   */
  @Test
  public void testAggressiveTombstoneReaping() {
    assumeTrue(getClass() == DistributedAckRegionCCEDUnitTest.class);

    final String name = this.getUniqueName() + "-CC";


    final int saveExpiredTombstoneLimit = TombstoneService.EXPIRED_TOMBSTONE_LIMIT;
    final long saveTombstoneTimeout = TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT;
    TombstoneService.EXPIRED_TOMBSTONE_LIMIT = 50;
    TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = 500;
    try {
      // create some destroyed entries so the GC service is populated
      RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
      CCRegion = (LocalRegion) f.create(name);
      final long initialCount = CCRegion.getCachePerfStats().getTombstoneGCCount();
      for (int i = 0; i < 100; i++) {
        CCRegion.put("cckey" + i, "ccvalue" + i);
        CCRegion.destroy("cckey" + i);
      }
      // now simulate a low free-memory condition
      TombstoneService.FORCE_GC_MEMORY_EVENTS = true;
      WaitCriterion waitForGC = new WaitCriterion() {
        public boolean done() {
          return CCRegion.getCachePerfStats().getTombstoneGCCount() > initialCount;
        }

        public String description() {
          return "waiting for GC to occur";
        }
      };
      Wait.waitForCriterion(waitForGC, 20000, 1000, true);
      Wait.pause(5000);
      long gcCount = CCRegion.getCachePerfStats().getTombstoneGCCount();
      assertTrue("expected a few GCs, but not " + (gcCount - initialCount),
          gcCount < (initialCount + 20));
    } catch (CacheException ex) {
      org.apache.geode.test.dunit.Assert.fail("While creating region", ex);
    } finally {
      TombstoneService.EXPIRED_TOMBSTONE_LIMIT = saveExpiredTombstoneLimit;
      TombstoneService.FORCE_GC_MEMORY_EVENTS = false;
      TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = saveTombstoneTimeout;
    }
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEventsOnEmptyRegion() {
    versionTestConcurrentEventsOnEmptyRegion();
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEventsOnNonReplicatedRegion() {
    versionTestConcurrentEventsOnNonReplicatedRegion();
  }

  @Test
  public void testGetAllWithVersions() {
    versionTestGetAllWithVersions();
  }

  @Test
  public void testEntryVersionRollover() throws Exception {
    assumeTrue(getClass() == DistributedAckRegionCCEDUnitTest.class);

    final String name = this.getUniqueName() + "-CC";
    final int numEntries = 1;
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
          CCRegion = (LocalRegion) f.create(name);
          for (int i = 0; i < numEntries; i++) {
            CCRegion.put("cckey" + i, "ccvalue");
          }
          assertEquals("expected no conflated events", 0,
              CCRegion.getCachePerfStats().getConflatedEventsCount());
        } catch (CacheException ex) {
          org.apache.geode.test.dunit.Assert.fail("While creating region", ex);
        }
      }
    };
    VM vm0 = Host.getHost(0).getVM(0);
    vm0.invoke(createRegion);
    try {
      createRegion.run();
      VersionTag tag = new VMVersionTag();
      // set the version to the max - it should make the system think there's a rollover and reject
      // the change. Then apply it to the cache as if it is a replayed client operation. That should
      // cause the cache to apply the op locally
      tag.setEntryVersion(0xFFFFFF);
      tag.setDistributedSystemId(1);
      tag.setRegionVersion(CCRegion.getVersionVector().getNextVersion());
      VersionTagHolder holder = new VersionTagHolder(tag);
      ClientProxyMembershipID id = ClientProxyMembershipID
          .getNewProxyMembership(CCRegion.getDistributionManager().getSystem());
      CCRegion.basicBridgePut("cckey0", "newvalue", null, true, null, id, true, holder);
      vm0.invoke(new SerializableRunnable("check conflation count") {
        public void run() {
          assertEquals("expected one conflated event", 1,
              CCRegion.getCachePerfStats().getConflatedEventsCount());
        }
      });
    } finally {
      disconnectAllFromDS();
    }
  }

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // these methods can be uncommented to inhibit test execution
  // when new tests are added
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  // @Override
  // public void testNonblockingGetInitialImage() throws Throwable {
  // }
  // @Override
  // public void testConcurrentOperations() throws Exception {
  // }
  //
  // @Override
  // public void testDistributedUpdate() {
  // }
  //
  // @Override
  // public void testDistributedGet() {
  // }
  //
  // @Override
  // public void testDistributedPutNoUpdate() throws InterruptedException {
  // }
  //
  // @Override
  // public void testDefinedEntryUpdated() {
  // }
  //
  // @Override
  // public void testDistributedDestroy() throws InterruptedException {
  // }
  //
  // @Override
  // public void testDistributedRegionDestroy() throws InterruptedException {
  // }
  //
  // @Override
  // public void testLocalRegionDestroy() throws InterruptedException {
  // }
  //
  // @Override
  // public void testDistributedInvalidate() {
  // }
  //
  // @Override
  // public void testDistributedInvalidate4() throws InterruptedException {
  // }
  //
  // @Override
  // public void testDistributedRegionInvalidate() throws InterruptedException {
  // }
  //
  // @Override
  // public void testRemoteCacheListener() throws InterruptedException {
  // }
  //
  // @Override
  // public void testRemoteCacheListenerInSubregion() throws InterruptedException {
  // }
  //
  // @Override
  // public void testRemoteCacheLoader() throws InterruptedException {
  // }
  //
  // @Override
  // public void testRemoteCacheLoaderArg() throws InterruptedException {
  // }
  //
  // @Override
  // public void testRemoteCacheLoaderException() throws InterruptedException {
  // }
  //
  // @Override
  // public void testCacheLoaderWithNetSearch() throws CacheException {
  // }
  //
  // @Override
  // public void testCacheLoaderWithNetLoad() throws CacheException {
  // }
  //
  // @Override
  // public void testNoRemoteCacheLoader() throws InterruptedException {
  // }
  //
  // @Override
  // public void testNoLoaderWithInvalidEntry() {
  // }
  //
  // @Override
  // public void testRemoteCacheWriter() throws InterruptedException {
  // }
  //
  // @Override
  // public void testLocalAndRemoteCacheWriters() throws InterruptedException {
  // }
  //
  // @Override
  // public void testCacheLoaderModifyingArgument() throws InterruptedException {
  // }
  //
  // @Override
  // public void testRemoteLoaderNetSearch() throws CacheException {
  // }
  //
  // @Override
  // public void testLocalCacheLoader() {
  // }
  //
  // @Override
  // public void testDistributedPut() throws Exception {
  // }
  //
  // @Override
  // public void testReplicate() throws InterruptedException {
  // }
  //
  // @Override
  // public void testDeltaWithReplicate() throws InterruptedException {
  // }
  //
  // @Override
  // public void testGetInitialImage() {
  // }
  //
  // @Override
  // public void testLargeGetInitialImage() {
  // }
  //
  // @Override
  // public void testMirroredDataFromNonMirrored() throws InterruptedException {
  // }
  //
  // @Override
  // public void testNoMirroredDataToNonMirrored() throws InterruptedException {
  // }
  //
  // @Override
  // public void testMirroredLocalLoad() {
  // }
  //
  // @Override
  // public void testMirroredNetLoad() {
  // }
  //
  // @Override
  // public void testNoRegionKeepAlive() throws InterruptedException {
  // }
  //
  // @Override
  // public void testNetSearchObservesTtl() throws InterruptedException {
  // }
  //
  // @Override
  // public void testNetSearchObservesIdleTime() throws InterruptedException {
  // }
  //
  // @Override
  // public void testEntryTtlDestroyEvent() throws InterruptedException {
  // }
  //
  // @Override
  // public void testUpdateResetsIdleTime() throws InterruptedException {
  // }
  // @Override
  // public void testTXNonblockingGetInitialImage() throws Throwable {
  // }
  //
  // @Override
  // public void testNBRegionInvalidationDuringGetInitialImage() throws Throwable {
  // }
  //
  // @Override
  // public void testNBRegionDestructionDuringGetInitialImage() throws Throwable {
  // }
  //
  // @Override
  // public void testNoDataSerializer() {
  // }
  //
  // @Override
  // public void testNoInstantiator() {
  // }
  //
  // @Override
  // public void testTXSimpleOps() throws Exception {
  // }
  //
  // @Override
  // public void testTXUpdateLoadNoConflict() throws Exception {
  // }
  //
  // @Override
  // public void testTXMultiRegion() throws Exception {
  // }
  //
  // @Override
  // public void testTXRmtMirror() throws Exception {
  // }


}
