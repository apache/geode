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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.OffHeapTestUtil;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Tests the basic functionality of the lru eviction controller and its statistics using off-heap
 * regions.
 *
 * @since Geode 1.0
 */
@Category({OffHeapTest.class})
public class OffHeapLRUEvictionControllerDUnitTest extends LRUEvictionControllerDUnitTest {

  public OffHeapLRUEvictionControllerDUnitTest() {
    super();
  }

  @Override
  public final void preTearDownAssertions() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if (hasCache()) {
          OffHeapTestUtil.checkOrphans(getCache());
        }
      }
    };
    Invoke.invokeInEveryVM(checkOrphans);
    checkOrphans.run();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.setProperty(OFF_HEAP_MEMORY_SIZE, "100m");

    return properties;
  }

  @Override
  protected boolean isOffHeapEnabled() {
    return true;
  }

  @Override
  protected HeapEvictor getEvictor() {
    return getCache().getOffHeapEvictor();
  }

  @Override
  protected ResourceType getResourceType() {
    return ResourceType.OFFHEAP_MEMORY;
  }
}
