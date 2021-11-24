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
package org.apache.geode.internal.cache.versions;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.cache.InitialImageOperation.GIITestHookType.DuringApplyDelta;
import static org.apache.geode.internal.cache.InitialImageOperation.resetAllGIITestHooks;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.DestroyOperation;
import org.apache.geode.internal.cache.DistributedTombstoneOperation;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;


public class TombstoneDUnitTest implements Serializable {
  private static final long serialVersionUID = 2992716917694662945L;
  private static Cache cache;
  private static Region<String, String> region;
  final String REGION_NAME = "TestRegion";

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @After
  public void close() {
    for (VM vm : Arrays.asList(VM.getVM(0), VM.getVM(1))) {
      vm.invoke(() -> {
        resetAllGIITestHooks();
        if (region != null && !region.isDestroyed()) {
          region.destroyRegion();
        }
        region = null;
        if (cache != null) {
          cache.close();
        }
      });
    }
  }

  @Test
  public void testTombstoneGcMessagesBetweenPersistentAndNonPersistentRegion() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    vm0.invoke(() -> {
      createCacheAndRegion(REPLICATE_PERSISTENT);
      region.put("K1", "V1");
      region.put("K2", "V2");
    });

    vm1.invoke(() -> createCacheAndRegion(REPLICATE));

    vm0.invoke(() -> {
      // Send tombstone gc message to vm1.
      region.destroy("K1");
      assertEquals(1, ((InternalCache) cache).getCachePerfStats().getTombstoneCount());
      performGC(1);
    });

    vm1.invoke(() -> {
      // After processing tombstone message from vm0. The tombstone count should be 0.
      waitForTombstoneCount(0);
      assertEquals(0, ((InternalCache) cache).getCachePerfStats().getTombstoneCount());

      // Send tombstone gc message to vm0.
      region.destroy("K2");
      performGC(1);
    });

    vm0.invoke(() -> {
      // After processing tombstone message from vm0. The tombstone count should be 0.
      waitForTombstoneCount(0);
      assertEquals(0, ((InternalCache) cache).getCachePerfStats().getTombstoneCount());
    });
  }


  @Test
  public void testWhenAnOutOfRangeTimeStampIsSeenWeExpireItInReplicateTombstoneSweeper() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    final int FAR_INTO_THE_FUTURE = 1000000; // 1 million millis into the future
    final int count = 10;

    // Create a cache and load some boiler plate entries
    server1.invoke(() -> {
      createCacheAndRegion(RegionShortcut.REPLICATE);
      for (int i = 0; i < count; i++) {
        region.put("K" + i, "V" + i);
      }
    });

    server2.invoke(() -> createCacheAndRegion(RegionShortcut.REPLICATE));

    server1.invoke(() -> {

      // Now that we have a cache and a region specifically with data, we can start the real work
      TombstoneService.TombstoneSweeper tombstoneSweeper =
          ((InternalCache) cache).getTombstoneService().getSweeper((LocalRegion) region);

      // Get one of the entries
      RegionEntry regionEntry = ((LocalRegion) region).getRegionEntry("K0");

      /*
       * Create a version tag with a timestamp far off in the future...
       * It should be in the near past, but we are testing that a future tombstone will be cleared
       */
      VersionTag<?> versionTag = regionEntry.getVersionStamp().asVersionTag();
      versionTag.setVersionTimeStamp(System.currentTimeMillis() + FAR_INTO_THE_FUTURE);

      // Create the forged tombstone with the versionTag from the future
      TombstoneService.Tombstone modifiedTombstone =
          new TombstoneService.Tombstone(regionEntry, (LocalRegion) region,
              versionTag);

      // Add it to the list of tombstones so that when checkOldestUnexpired is called it will see it
      tombstoneSweeper.tombstones.add(modifiedTombstone);
      tombstoneSweeper.checkOldestUnexpired(System.currentTimeMillis());

      // Validate that the tombstone was cleared.
      assertThat(tombstoneSweeper.getOldestTombstoneTime()).isEqualTo(0);
    });
  }

  @Test
  public void testWhenAnOutOfRangeTimeStampIsSeenWeExpireItInNonReplicateTombstoneSweeper() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    final int FAR_INTO_THE_FUTURE = 1000000; // 1 million millis into the future
    final int count = 2000;
    Logger logger = LogService.getLogger();
    // Create a cache and load some boiler plate entries
    server1.invoke(() -> {
      createCacheAndRegion(RegionShortcut.PARTITION);
      for (int i = 0; i < count; i++) {
        region.put("K" + i, "V" + i);
      }
    });

    server2.invoke(() -> createCacheAndRegion(RegionShortcut.PARTITION));

    server1.invoke(() -> {

      // Now that we have a cache and a region specifically with data, we can start the real work
      TombstoneService.TombstoneSweeper tombstoneSweeper =
          ((InternalCache) cache).getTombstoneService().getSweeper((LocalRegion) region);

      // Get one of the entries

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      RegionEntry regionEntry = partitionedRegion.getBucketRegion("K0").getRegionEntry("K0");

      /*
       * Create a version tag with a timestamp far off in the future...
       * It should be in the near past, but we are testing that a future tombstone will be cleared
       */

      VersionTag<?> versionTag = regionEntry.getVersionStamp().asVersionTag();
      versionTag.setVersionTimeStamp(System.currentTimeMillis() + FAR_INTO_THE_FUTURE);

      // Create the forged tombstone with the versionTag from the future
      TombstoneService.Tombstone modifiedTombstone =
          new TombstoneService.Tombstone(regionEntry, (LocalRegion) region,
              versionTag);

      // Add it to the list of tombstones so that when checkOldestUnexpired is called it will see it
      tombstoneSweeper.tombstones.add(modifiedTombstone);
      tombstoneSweeper.checkOldestUnexpired(System.currentTimeMillis());

      // Validate that the tombstone was cleared.
      assertThat(tombstoneSweeper.getOldestTombstoneTime()).isEqualTo(0);
    });
  }



  @Test
  public void testGetOldestTombstoneTimeForReplicateTombstoneSweeper() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    final int count = 10;
    server1.invoke(() -> {
      createCacheAndRegion(RegionShortcut.REPLICATE);
      for (int i = 0; i < count; i++) {
        region.put("K" + i, "V" + i);
      }
    });

    server2.invoke(() -> createCacheAndRegion(RegionShortcut.REPLICATE));

    server1.invoke(() -> {
      TombstoneService.TombstoneSweeper tombstoneSweeper =
          ((InternalCache) cache).getTombstoneService().getSweeper((LocalRegion) region);
      // Send tombstone gc message to vm1.
      for (int i = 0; i < count; i++) {
        region.destroy("K" + i);
        await().untilAsserted(() -> assertThat(
            tombstoneSweeper.getOldestTombstoneTime()
                + TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT - System.currentTimeMillis())
                    .isGreaterThan(0));
        performGC(1);
      }

      assertThat(tombstoneSweeper.getOldestTombstoneTime()).isEqualTo(0);
    });
  }

  @Test
  public void testGetOldestTombstoneTimeForNonReplicateTombstoneSweeper() {
    VM client = VM.getVM(0);
    VM server = VM.getVM(1);


    // Fire up the server and put in some data that is deletable
    server.invoke(() -> {
      createCacheAndRegion(REPLICATE);
      int serverPort = getRandomAvailableTCPPort();
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(serverPort);
      cacheServer.start();
      for (int i = 0; i < 1000; i++) {
        region.put("K" + i, "V" + i);
      }
    });

    String locatorHost = NetworkUtils.getServerHostName();
    int locatorPort = DistributedRule.getLocatorPort();
    // Use the client to remove and entry, thus creating a tombstone
    client.invoke(() -> {
      createClientCacheAndRegion(locatorHost, locatorPort);
      region.remove("K3");
    });

    // Validate that a tombstone was created and that it has a timestamp that is valid,
    // Then GC and validate there is no oldest tombstone.
    server.invoke(() -> {
      TombstoneService.TombstoneSweeper tombstoneSweeper =
          ((InternalCache) cache).getTombstoneService().getSweeper((LocalRegion) region);

      assertThat(tombstoneSweeper.getOldestTombstoneTime()).isGreaterThan(0)
          .isLessThan(((InternalCache) cache).cacheTimeMillis());
      performGC(1);
      assertThat(tombstoneSweeper.getOldestTombstoneTime()).isEqualTo(0);
    });
  }

  /**
   * The purpose of this test is for a replicate region scenario to get the oldest tombstone
   * and validate that it matches the tombstone of the entry we removed.
   */
  @Test
  public void testGetOldestTombstoneForReplicateTombstoneSweeper() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);

    server1.invoke(() -> {
      createCacheAndRegion(REPLICATE);
      region.put("K1", "V1");
      region.put("K2", "V2");
    });

    server2.invoke(() -> createCacheAndRegion(REPLICATE));

    server1.invoke(() -> {
      // Send tombstone gc message to vm1.
      region.destroy("K1");

      TombstoneService.TombstoneSweeper tombstoneSweeper =
          ((InternalCache) cache).getTombstoneService().getSweeper((LocalRegion) region);

      assertThat(tombstoneSweeper.getOldestTombstone()).contains("K1");
      performGC(1);
      assertThat(tombstoneSweeper.getOldestTombstone()).isNull();
    });
  }


  /**
   * The purpose of this test is for a non-replicate region scenario to get the oldest tombstone
   * and validate that it matches the tombstone of the entry we removed. This is done through a
   * client
   * as a client is required to have this non-replicate tombstone.
   */
  @Test
  public void testGetOldestTombstoneForNonReplicateTombstoneSweeper() {
    VM client = VM.getVM(0);
    VM server = VM.getVM(1);

    // Fire up the server and put in some data that is deletable
    server.invoke(() -> {
      createCacheAndRegion(REPLICATE);
      int serverPort = getRandomAvailableTCPPort();
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(serverPort);
      cacheServer.start();
      for (int i = 0; i < 1000; i++) {
        region.put("K" + i, "V" + i);
      }
    });

    String locatorHost = NetworkUtils.getServerHostName();
    int locatorPort = DistributedRule.getLocatorPort();
    // Use the client to remove and entry, thus creating a tombstone
    client.invoke(() -> {
      createClientCacheAndRegion(locatorHost, locatorPort);
      region.remove("K3");
    });

    // Validate that a tombstone was created and that it has a timestamp that is valid,
    // Then GC and validate there is no oldest tombstone.
    server.invoke(() -> {
      TombstoneService.TombstoneSweeper tombstoneSweeper =
          ((InternalCache) cache).getTombstoneService().getSweeper((LocalRegion) region);
      assertThat(tombstoneSweeper.getOldestTombstone()).contains("K3");
      performGC(1);
      assertThat(tombstoneSweeper.getOldestTombstone()).isNull();
    });
  }

  @Test
  public void testTombstonesWithLowerVersionThanTheRecordedVersionGetsGCedAfterDestroy()
      throws Exception {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    Properties props = DistributedRule.getDistributedSystemProperties();
    props.setProperty("conserve-sockets", "false");

    vm0.invoke(() -> {
      createCacheAndRegion(REPLICATE_PERSISTENT, props);
      region.put("K1", "V1");
      region.put("K2", "V2");
    });

    vm1.invoke(() -> {
      createCacheAndRegion(REPLICATE, props);
      DistributionMessageObserver.setInstance(new RegionObserver());
    });

    AsyncInvocation<Object> vm0Async1 = vm0.invokeAsync(() -> {
      region.destroy("K1");
    });

    AsyncInvocation<Object> vm0Async2 = vm0.invokeAsync(() -> {
      region.destroy("K2");
    });

    AsyncInvocation<Object> vm0Async3 = vm0.invokeAsync(() -> {
      waitForTombstoneCount(2);
      performGC(2);
    });

    vm1.invoke(() -> await()
        .until(() -> ((InternalCache) cache).getCachePerfStats().getTombstoneGCCount() == 1));

    vm0Async1.await();
    vm0Async2.await();
    vm0Async3.await();

    vm1.invoke(() -> {
      performGC(((LocalRegion) region).getTombstoneCount());
      assertEquals(0, ((LocalRegion) region).getTombstoneCount());
    });
  }

  @Test
  public void tombstoneGCDuringGIICorrectlySchedulesTombstonesForCollection() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    Properties props = DistributedRule.getDistributedSystemProperties();
    props.setProperty("conserve-sockets", "false");

    vm0.invoke(() -> {
      createCacheAndRegion(REPLICATE_PERSISTENT, props);
      region.put("K1", "V1");
      region.put("K2", "V2");
    });

    vm1.invoke(() -> {
      createCacheAndRegion(REPLICATE_PERSISTENT, props);
      // Ensure that there are local tombstones to be recovered in the member that will request GII
      region.destroy("K1");
      region.destroy("K2");
      cache.close();
      region = null;
    });

    vm0.invoke(() -> {
      // Ensure that there are newer tombstones that will be sent via GII
      region.put("K1", "V3");
      region.destroy("K1");
      region.put("K2", "V4");
      region.destroy("K2");
      // Trigger a tombstone GC after receiving the GII request message
      InitialImageOperation.setGIITestHook(new InitialImageOperation.GIITestHook(
          InitialImageOperation.GIITestHookType.AfterReceivedRequestImage, REGION_NAME) {
        private static final long serialVersionUID = -3790198435185240444L;

        @Override
        public void reset() {}

        @Override
        public void run() {
          try {
            performGC(((LocalRegion) region).getTombstoneCount());
          } catch (Exception ignore) {
          }
        }
      });
    });

    vm1.invoke(() -> {
      cache = new CacheFactory(props).create();

      InitialImageOperation.setGIITestHook(
          new InitialImageOperation.GIITestHook(DuringApplyDelta, REGION_NAME) {
            private static final long serialVersionUID = 637083883125364247L;
            private int entryNumber = 0;

            @Override
            public void reset() {}

            @Override
            public void run() {
              if (entryNumber == 0) {
                await().alias("Waiting for scheduled tombstone count to be zero")
                    .until(() -> ((InternalCache) cache).getTombstoneService()
                        .getScheduledTombstoneCount() == 0);
              }
              // Confirm that tombstones are correctly scheduled for collection after processing
              // each new entry received during GII
              assertThat(((InternalCache) cache).getTombstoneService().getScheduledTombstoneCount())
                  .as("Scheduled tombstone count did not match expected value")
                  .isEqualTo(entryNumber++);
            }
          });

      region = cache.<String, String>createRegionFactory(REPLICATE_PERSISTENT).create(REGION_NAME);

      // Confirm that we are able to collect all tombstones once the region is initialized
      performGC(((LocalRegion) region).getTombstoneCount());
      assertEquals(0, ((LocalRegion) region).getTombstoneCount());
    });
  }

  // Client Cache
  private void createClientCacheAndRegion(String locatorHost, int locatorPort) {
    ClientCache clientcache =
        new ClientCacheFactory().addPoolLocator(locatorHost, locatorPort).create();
    region = clientcache.<String, String>createClientRegionFactory(
        ClientRegionShortcut.PROXY).create(REGION_NAME);
  }

  // Server Cache
  private void createCacheAndRegion(RegionShortcut replicatePersistent, Properties properties) {
    cache = (new CacheFactory(properties)).create();
    region = cache.<String, String>createRegionFactory(replicatePersistent).create(REGION_NAME);
  }

  private void createCacheAndRegion(RegionShortcut regionShortCut) {
    createCacheAndRegion(regionShortCut, DistributedRule.getDistributedSystemProperties());
  }

  private static class RegionObserver extends DistributionMessageObserver implements Serializable {
    private static final long serialVersionUID = 6272522949825923089L;
    VersionTag<?> versionTag;
    CountDownLatch tombstoneGcLatch = new CountDownLatch(1);

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      // Allow destroy with higher version to complete first.
      if (message instanceof DestroyOperation.DestroyMessage) {
        // wait for tombstoneGC message to complete.
        try {
          tombstoneGcLatch.await();
          synchronized (this) {
            DestroyOperation.DestroyMessage destroyMessage =
                (DestroyOperation.DestroyMessage) message;
            if (versionTag == null) {
              // First destroy
              versionTag = destroyMessage.getVersionTag();
              wait();
            } else {
              // Second destroy
              if (destroyMessage.getVersionTag().getRegionVersion() < versionTag
                  .getRegionVersion()) {
                notifyAll();
                wait();
              }
            }
          }
        } catch (InterruptedException ignored) {
        }
      }
    }

    @Override
    public void afterProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof DestroyOperation.DestroyMessage) {
        // Notify the destroy with smaller version to continue.
        synchronized (this) {
          notifyAll();
        }
      }
      if (message instanceof DistributedTombstoneOperation.TombstoneMessage) {
        tombstoneGcLatch.countDown();
      }
    }
  }

  private void waitForTombstoneCount(int count) {
    try {
      await().until(() -> ((InternalCache) cache).getCachePerfStats().getTombstoneCount() == count);
    } catch (Exception e) {
      // The caller to throw exception with proper message.
    }
  }

  private void performGC(int count) throws Exception {
    ((InternalCache) cache).getTombstoneService().forceBatchExpirationForTests(count);
  }

}
