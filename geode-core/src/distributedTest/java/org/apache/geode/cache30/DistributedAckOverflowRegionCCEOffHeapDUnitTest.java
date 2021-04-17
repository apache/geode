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

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.OffHeapTestUtil;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Tests Distributed Ack Overflow Region with ConcurrencyChecksEnabled and OffHeap memory.
 *
 * @since Geode 1.0
 */
@SuppressWarnings({"deprecation", "serial"})
@Category({OffHeapTest.class})
public class DistributedAckOverflowRegionCCEOffHeapDUnitTest
    extends DistributedAckOverflowRegionCCEDUnitTest {

  public DistributedAckOverflowRegionCCEOffHeapDUnitTest() {
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
    checkOrphans.run();
    Invoke.invokeInEveryVM(checkOrphans);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "10m");
    return props;
  }

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    RegionAttributes<K, V> attrs = super.getRegionAttributes();
    AttributesFactory<K, V> factory = new AttributesFactory<>(attrs);
    factory.setOffHeap(true);
    return factory.create();
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    RegionAttributes<K, V> ra = super.getRegionAttributes(type);
    AttributesFactory<K, V> factory = new AttributesFactory<>(ra);
    if (!ra.getDataPolicy().isEmpty()) {
      factory.setOffHeap(true);
    }
    return factory.create();
  }
}
