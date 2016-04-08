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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests PartitionedRegion DataStore currentAllocatedMemory operation.
 * 
 * @since 9.0
 */
@Category(IntegrationTest.class)
public class PRDataStoreMemoryOffHeapJUnitTest extends PRDataStoreMemoryJUnitTest {

  @Override
  protected Properties getDistributedSystemProperties() {
    Properties dsProps = super.getDistributedSystemProperties();
    dsProps.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
    return dsProps;
  }
  
  @SuppressWarnings({ "rawtypes", "deprecation" })
  @Override
  protected RegionFactory<?, ?> defineRegionFactory() {
    return new RegionFactory()
        .setPartitionAttributes(definePartitionAttributes())
        .setOffHeap(true);
  }
}
