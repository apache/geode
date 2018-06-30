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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class TombstoneDUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testTombstoneGcMessagesBetweenPersistnentAndNonPersistentRegion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(() -> {
      createRegion("TestRegion", true);
      Region r = getCache().getRegion("TestRegion");
      r.put("K1", "V1");
      r.put("K2", "V2");
    });

    vm1.invoke(() -> {
      createRegion("TestRegion", false);
    });

    vm0.invoke(() -> {
      // Send tombstone gc message to vm1.
      Region r = getCache().getRegion("TestRegion");
      r.destroy("K1");
      assertEquals(1, getGemfireCache().getCachePerfStats().getTombstoneCount());
      performGC(r);
    });

    vm1.invoke(() -> {
      // After processing tombstone message from vm0. The tombstone count should be 0.
      waitForTombstoneCount(0);
      assertEquals(0, getGemfireCache().getCachePerfStats().getTombstoneCount());

      // Send tombstone gc message to vm0.
      Region r = getCache().getRegion("TestRegion");
      r.destroy("K2");
      performGC(r);
    });

    vm0.invoke(() -> {
      // After processing tombstone message from vm0. The tombstone count should be 0.
      waitForTombstoneCount(0);
      assertEquals(0, getGemfireCache().getCachePerfStats().getTombstoneCount());
    });
  }

  private void waitForTombstoneCount(int count) {
    try {
      Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
        return getGemfireCache().getCachePerfStats().getTombstoneCount() == count;
      });
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

  private void performGC(Region r) throws Exception {
    getGemfireCache().getTombstoneService().forceBatchExpirationForTests(1);
  }

}
