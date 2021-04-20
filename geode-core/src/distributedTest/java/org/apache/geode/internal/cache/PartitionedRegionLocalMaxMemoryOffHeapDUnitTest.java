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

import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Tests PartitionedRegion localMaxMemory with Off-Heap memory.
 *
 * @since Geode 1.0
 */
@Category({OffHeapTest.class})
@SuppressWarnings("serial")
public class PartitionedRegionLocalMaxMemoryOffHeapDUnitTest
    extends PartitionedRegionLocalMaxMemoryDUnitTest {

  @Override
  public final void preTearDownAssertions() throws Exception {
    checkOrphans();
    invokeInEveryVM(() -> checkOrphans());
  }

  private void checkOrphans() {
    if (hasCache()) {
      OffHeapTestUtil.checkOrphans(getCache());
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    // test creates a bit more than 1m of off heap so we need to total off heap size to be >1m
    config.setProperty(OFF_HEAP_MEMORY_SIZE, "2m");
    return config;
  }

  @Override
  protected RegionAttributes<?, ?> createRegionAttrsForPR(int redundancy, int localMaxMemory,
      long recoveryDelay, EvictionAttributes evictionAttributes) {
    RegionAttributes<?, ?> regionAttributes = PartitionedRegionTestHelper.createRegionAttrsForPR(
        redundancy, localMaxMemory, recoveryDelay, evictionAttributes, null);
    AttributesFactory<?, ?> attributesFactory = new AttributesFactory<>(regionAttributes);
    attributesFactory.setOffHeap(true);
    return attributesFactory.create();
  }
}
