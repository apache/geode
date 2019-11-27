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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ClusterMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.DestroyOperation;
import org.apache.geode.internal.cache.DistributedTombstoneOperation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class TombstoneDUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testTombstoneGcMessagesBetweenPersistnentAndNonPersistentRegion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(() -> {
      createRegion("TestRegion", true);
      Region<String, String> region = getCache().getRegion("TestRegion");
      region.put("K1", "V1");
      region.put("K2", "V2");
    });

    vm1.invoke(() -> {
      createRegion("TestRegion", false);
    });

    vm0.invoke(() -> {
      // Send tombstone gc message to vm1.
      Region<String, String> region = getCache().getRegion("TestRegion");
      region.destroy("K1");
      assertEquals(1, getGemfireCache().getCachePerfStats().getTombstoneCount());
      performGC();
    });

    vm1.invoke(() -> {
      // After processing tombstone message from vm0. The tombstone count should be 0.
      waitForTombstoneCount(0);
      assertEquals(0, getGemfireCache().getCachePerfStats().getTombstoneCount());

      // Send tombstone gc message to vm0.
      Region<String, String> region = getCache().getRegion("TestRegion");
      region.destroy("K2");
      performGC();
    });

    vm0.invoke(() -> {
      // After processing tombstone message from vm0. The tombstone count should be 0.
      waitForTombstoneCount(0);
      assertEquals(0, getGemfireCache().getCachePerfStats().getTombstoneCount());
    });
  }

  @Test
  public void testTombstonesWithLowerVersionThanTheRecordedVersionGetsGCed() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createCache(vm0);
    createCache(vm1);

    vm0.invoke(() -> {
      createRegion("TestRegion", true);
      Region<String, String> region = getCache().getRegion("TestRegion");
      region.put("K1", "V1");
      region.put("K2", "V2");
    });

    vm1.invoke(() -> {
      createRegion("TestRegion", false);
      DistributionMessageObserver.setInstance(new RegionObserver());
    });

    AsyncInvocation vm0Async1 = vm0.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion("TestRegion");
      region.destroy("K1");
    });

    AsyncInvocation vm0Async2 = vm0.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion("TestRegion");
      region.destroy("K2");
    });

    AsyncInvocation vm0Async3 = vm0.invokeAsync(() -> {
      waitForTombstoneCount(2);
      performGC(2);
    });

    vm1.invoke(() -> {
      await().until(() -> getCache().getCachePerfStats().getTombstoneGCCount() == 1);
    });

    vm0Async1.join();
    vm0Async2.join();
    vm0Async3.join();

    vm1.invoke(() -> {
      Region<String, String> region = getCache().getRegion("TestRegion");
      performGC(((LocalRegion) region).getTombstoneCount());
      assertEquals(0, ((LocalRegion) region).getTombstoneCount());
    });
  }

  private class RegionObserver extends DistributionMessageObserver implements Serializable {

    VersionTag versionTag = null;
    CountDownLatch tombstoneGcLatch = new CountDownLatch(1);

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, ClusterMessage message) {
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
              this.wait();
            } else {
              // Second destroy
              if (destroyMessage.getVersionTag().getRegionVersion() < versionTag
                  .getRegionVersion()) {
                this.notifyAll();
                this.wait();
              }
            }
          }
        } catch (InterruptedException ex) {
        }
      }
    }

    @Override
    public void afterProcessMessage(ClusterDistributionManager dm, ClusterMessage message) {
      if (message instanceof DestroyOperation.DestroyMessage) {
        // Notify the destroy with smaller version to continue.
        synchronized (this) {
          this.notifyAll();
        }
      }
      if (message instanceof DistributedTombstoneOperation.TombstoneMessage) {
        tombstoneGcLatch.countDown();
      }
    }
  };

  private void createCache(VM vm) {
    vm.invoke(() -> {
      if (cache != null && !cache.isClosed()) {
        cache.close();
      }
      Properties props = new Properties();
      props.put("conserve-sockets", "false");
      cache = getCache(props);
    });
  }

  private void waitForTombstoneCount(int count) {
    try {
      await().until(() -> getCache().getCachePerfStats().getTombstoneCount() == count);
    } catch (Exception e) {
      // The caller to throw exception with proper message.
    }
  }

  private void createRegion(String regionName, boolean persistent) {
    if (persistent) {
      getCache().createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(regionName);
    } else {
      getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    }
  }

  private void performGC(int count) throws Exception {
    getCache().getTombstoneService().forceBatchExpirationForTests(count);
  }

  private void performGC() throws Exception {
    performGC(1);
  }
}
