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

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.OffHeapTestUtil;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;

/**
 * Tests Distributed Ack Region with ConcurrencyChecksEnabled and OffHeap memory.
 *
 * @since Geode 1.0
 */

@SuppressWarnings({"deprecation", "serial"})
public class DistributedNoAckRegionCCEOffHeapDUnitTest extends DistributedNoAckRegionCCEDUnitTest {

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
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "10m");
    return props;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  protected RegionAttributes getRegionAttributes() {
    RegionAttributes attrs = super.getRegionAttributes();
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setOffHeap(true);
    return factory.create();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = super.getRegionAttributes(type);
    AttributesFactory factory = new AttributesFactory(ra);
    if (!ra.getDataPolicy().isEmpty()) {
      factory.setOffHeap(true);
    }
    return factory.create();
  }
}
