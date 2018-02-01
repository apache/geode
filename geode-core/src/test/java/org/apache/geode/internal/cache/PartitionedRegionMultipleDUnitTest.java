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

import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This test is dunit test for the multiple Partition Regions in 4 VMs.
 */
@Category(DistributedTest.class)
public class PartitionedRegionMultipleDUnitTest extends CacheTestCase {

  private static final int REDUNDANCY = 0;
  private static final int LOCAL_MAX_MEMORY = 200;
  private static final String TEST_REGION = "testRegion";
  private static final String RETRY_TIMEOUT_VALUE = "20000";

  private static VM vm0;
  private static VM vm1;

  @Rule
  public DistributedRestoreSystemProperties restore = new DistributedRestoreSystemProperties();

  private int startIndexForKey = 0;
  private int endIndexForKey = 50;

  private int startIndexForDestroy = 20;
  private int endIndexForDestroy = 40;

  @BeforeClass
  public static void setup() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
  }

  /**
   * This test performs following operations:
   *
   * <p>
   * 1. Create multiple Partition Regions in 4 VMs
   *
   * <p>
   * 2. Performs put()operations on all the partitioned region from all the VM's
   *
   * <p>
   * 3. Performs destroy(key)operations for some of the keys of all the partitioned region from all
   * the VM's
   *
   * <p>
   * 4. Checks containsKey and ContainsValueForKey APIs
   */
  @Test
  public void testPartitionedRegionDestroyAndContainsAPI() throws Exception {
    vm0.invoke(() -> createPartitionRegion());
    vm1.invoke(() -> createPartitionRegion());

    vm0.invoke(() -> putInPartitionRegion(startIndexForKey, endIndexForKey));
    vm0.invoke(() -> destroyInPartitionedRegion());

    vm0.invoke(() -> validateContainsAPIForPartitionRegion());
    vm1.invoke(() -> validateContainsAPIForPartitionRegion());
  }

  private void putInPartitionRegion(int startIndexForKey, int endIndexForKey) {
    Region region = getCache().getRegion(Region.SEPARATOR + TEST_REGION);
    for (int k = startIndexForKey; k < endIndexForKey; k++) {
      region.put(TEST_REGION + k, TEST_REGION + k);
    }
  }

  private void destroyInPartitionedRegion() {
    Region region = getCache().getRegion(Region.SEPARATOR + TEST_REGION);
    for (int i = startIndexForDestroy; i < endIndexForDestroy; i++) {
      region.destroy(TEST_REGION + i);
    }
  }

  private void validateContainsAPIForPartitionRegion() {
    Cache cache = getCache();
    Region region = cache.getRegion(Region.SEPARATOR + TEST_REGION);

    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      Object val = region.get(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertThat(val).isNull();
      } else if (val != null) {
        assertEquals(val, TEST_REGION + i);
        assertTrue(region.containsValue(TEST_REGION + i));
        // pass
      } else {
        fail("Validation failed for key = " + TEST_REGION + i + "Value got = "
            + val);
      }
    }

    // containsKey
    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      boolean conKey = region.containsKey(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertFalse(conKey);
      } else {
        assertTrue(conKey);
      }
    }

    // containsValueForKey
    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      boolean conKey = region.containsValueForKey(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertFalse(conKey);
      } else {
        assertTrue(conKey);
      }
    }

    // containsValue
    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      boolean conKey = region.containsValue(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertFalse(conKey);
      } else {
        assertTrue(conKey);
      }
    }
  }

  private void createPartitionRegion() {
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, RETRY_TIMEOUT_VALUE);
    getCache().createRegion(TEST_REGION,
        createRegionAttrsForPR(REDUNDANCY, LOCAL_MAX_MEMORY,
            PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT));
  }

  protected RegionAttributes<?, ?> createRegionAttrsForPR(int red, int localMaxMem,
      long recoveryDelay) {
    return PartitionedRegionTestHelper.createRegionAttrsForPR(red, localMaxMem, recoveryDelay,
        null, null);
  }
}
