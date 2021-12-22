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

import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Iterating over Region.entrySet() should not return null for PartitionedRegion.
 *
 * <p>
 * TRAC #45164: Iterating on Region.entrySet() can return null on partitioned region
 */

public class PREntrySetIteratorRegressionTest extends CacheTestCase {

  private static final int ENTRY_COUNT = 10_000;
  private static final int ENTRY_DESTROY_SEQUENCE = 3;

  private String uniqueName;

  private VM vm1;
  private VM vm2;

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());

    vm1.invoke(this::doPuts);
  }

  @Test
  public void regionEntrySetIteratorNextShouldNeverReturnNull() throws Exception {
    AsyncInvocation destroySomeEntries = vm1.invokeAsync(() -> {
      Region<Integer, Object> region = getCache().getRegion(uniqueName);
      for (int j = 0; j < ENTRY_COUNT / ENTRY_DESTROY_SEQUENCE; j += ENTRY_DESTROY_SEQUENCE) {
        region.destroy(j);
      }
    });

    AsyncInvocation validateEntrySetIteratorContainsNoNulls = vm2.invokeAsync(() -> {
      Region<Integer, Object> region = getCache().getRegion(uniqueName);
      for (Entry<Integer, Object> entry : region.entrySet()) {
        assertThat(entry).isNotNull();
      }
    });

    destroySomeEntries.await();
    validateEntrySetIteratorContainsNoNulls.await();
  }

  private void createPartitionedRegion() {
    getCache().<Integer, Object>createRegionFactory(RegionShortcut.PARTITION).create(uniqueName);
  }

  private void doPuts() {
    Region<Integer, Object> region = getCache().getRegion(uniqueName);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      region.put(i, i);
    }
  }
}
