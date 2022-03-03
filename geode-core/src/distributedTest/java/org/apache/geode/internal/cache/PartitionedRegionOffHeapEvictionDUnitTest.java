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

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.OffHeapMemoryMonitor;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.OffHeapTest;

@Category({OffHeapTest.class})
public class PartitionedRegionOffHeapEvictionDUnitTest extends PartitionedRegionEvictionDUnitTest {

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
  protected void setEvictionPercentage(float percentage) {
    getCache().getResourceManager().setEvictionOffHeapPercentage(percentage);
  }

  @Override
  protected boolean isOffHeap() {
    return true;
  }

  @Override
  protected ResourceType getMemoryType() {
    return ResourceType.OFFHEAP_MEMORY;
  }

  @Override
  protected HeapEvictor getEvictor(Region region) {
    return ((GemFireCacheImpl) region.getRegionService()).getOffHeapEvictor();
  }

  @Override
  protected void raiseFakeNotification() {
    getCache().getOffHeapEvictor().setTestAbortAfterLoopCount(1);

    setEvictionPercentage(85);
    OffHeapMemoryMonitor ohmm =
        getCache().getInternalResourceManager().getOffHeapMonitor();
    ohmm.stopMonitoring(true);

    ohmm.updateStateAndSendEvent(94371840);
  }

  @Override
  protected void cleanUpAfterFakeNotification() {
    getCache().getOffHeapEvictor()
        .setTestAbortAfterLoopCount(Integer.MAX_VALUE);
  }
}
