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

import org.junit.After;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.OffHeapTestUtil;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Tests Global Region with ConcurrencyChecksEnabled and OffHeap memory.
 *
 * @since Geode 1.0
 */
@Category({OffHeapTest.class})
@SuppressWarnings({"serial"})
public class GlobalRegionCCEOffHeapDUnitTest extends GlobalRegionCCEDUnitTest {

  @After
  public void tearDown() throws Exception {
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
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "10m");
    return props;
  }

  private <K, V> RegionAttributes<K, V> getBasicAttributes(
      RegionAttributes<K, V> regionAttributes) {
    AttributesFactory<K, V> factory = new AttributesFactory<>(regionAttributes);
    if (!regionAttributes.getDataPolicy().withStorage()) {
      factory.setOffHeap(true);
    }
    return factory.create();
  }

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    RegionAttributes<K, V> attrs = super.getRegionAttributes();
    return getBasicAttributes(attrs);
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    RegionAttributes<K, V> ra = super.getRegionAttributes(type);
    return getBasicAttributes(ra);
  }
}
