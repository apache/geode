/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * Tests Distributed Ack Region with OffHeap memory.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
@SuppressWarnings({ "deprecation", "serial" })
public class DistributedAckRegionOffHeapDUnitTest extends DistributedAckRegionDUnitTest {

  public DistributedAckRegionOffHeapDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    Invoke.invokeInEveryVM(checkOrphans);
    checkOrphans.run();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "10m");
    return props;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes() {
    RegionAttributes attrs = super.getRegionAttributes();
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setOffHeap(true);
    return factory.create();
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = super.getRegionAttributes(type);
    AttributesFactory factory = new AttributesFactory(ra);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setOffHeap(true);
    }
    return factory.create();
  }
}
