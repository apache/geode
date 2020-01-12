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

import static org.apache.geode.cache.PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

/**
 * This test is dunit test for the multiple Partition Regions in 4 VMs.
 */

public class PartitionedRegionMultipleDUnitTest extends CacheTestCase {

  private static final int REDUNDANCY = 0;
  private static final int LOCAL_MAX_MEMORY = 200;
  private static final String TEST_REGION = "testRegion";
  private static final String RETRY_TIMEOUT_VALUE = "20000";

  private VM vm0;
  private VM vm1;

  private int startIndexForKey = 0;
  private int endIndexForKey = 50;

  private int startIndexForDestroy = 20;
  private int endIndexForDestroy = 40;

  @Rule
  public DistributedRestoreSystemProperties restore = new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
  }

  /**
   * This test performs following operations:<br>
   *
   * 1. Create multiple Partition Regions in 4 VMs<br>
   *
   * 2. Performs put()operations on all the partitioned region from all the VM's<br>
   *
   * 3. Performs destroy(key)operations for some of the keys of all the partitioned region from all
   * the VM's<br>
   *
   * 4. Checks containsKey and ContainsValueForKey APIs
   */
  @Test
  public void testPartitionedRegionDestroyAndContainsAPI() throws Exception {
    vm0.invoke(this::createPartitionRegion);
    vm1.invoke(this::createPartitionRegion);

    vm0.invoke(() -> putInPartitionRegion(startIndexForKey, endIndexForKey));
    vm0.invoke(this::destroyInPartitionedRegion);

    vm0.invoke(this::validateContainsAPIForPartitionRegion);
    vm1.invoke(this::validateContainsAPIForPartitionRegion);
  }

  private void putInPartitionRegion(int startIndexForKey, int endIndexForKey) {
    Region<String, String> region = getCache().getRegion(TEST_REGION);
    for (int k = startIndexForKey; k < endIndexForKey; k++) {
      region.put(TEST_REGION + k, TEST_REGION + k);
    }
  }

  private void destroyInPartitionedRegion() {
    Region region = getCache().getRegion(TEST_REGION);
    for (int i = startIndexForDestroy; i < endIndexForDestroy; i++) {
      region.destroy(TEST_REGION + i);
    }
  }

  private void validateContainsAPIForPartitionRegion() {
    Cache cache = getCache();
    Region<String, String> region = cache.getRegion(TEST_REGION);

    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      Object value = region.get(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertThat(value).isNull();
      } else {
        assertThat(value).isNotNull();
        assertThat(value).isEqualTo(TEST_REGION + i);
        assertThat(region).containsValue(TEST_REGION + i);
      }
    }

    // containsKey
    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      boolean containsKey = region.containsKey(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertThat(containsKey).isFalse();
      } else {
        assertThat(containsKey).isTrue();
      }
    }

    // containsValueForKey
    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      boolean containsValueForKey = region.containsValueForKey(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertThat(containsValueForKey).isFalse();
      } else {
        assertThat(containsValueForKey).isTrue();
      }
    }

    // containsValue
    for (int i = startIndexForKey; i < endIndexForKey; i++) {
      boolean containsValue = region.containsValue(TEST_REGION + i);
      if (i >= startIndexForDestroy && i < endIndexForDestroy) {
        assertThat(containsValue).isFalse();
      } else {
        assertThat(containsValue).isTrue();
      }
    }
  }

  private void createPartitionRegion() {
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, RETRY_TIMEOUT_VALUE);
    getCache().createRegion(TEST_REGION,
        createRegionAttrsForPR(REDUNDANCY, LOCAL_MAX_MEMORY, RECOVERY_DELAY_DEFAULT));
  }

  protected RegionAttributes<?, ?> createRegionAttrsForPR(int redundancy, int localMaxMemory,
      long recoveryDelay) {
    return PartitionedRegionTestHelper.createRegionAttrsForPR(redundancy, localMaxMemory,
        recoveryDelay, null, null);
  }
}
