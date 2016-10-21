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

import org.apache.geode.cache.RegionFactory;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * Tests PartitionedRegion DataStore currentAllocatedMemory operation.
 * 
 * @since Geode 1.0
 */
@Category(IntegrationTest.class)
public class PRDataStoreMemoryOffHeapJUnitTest extends PRDataStoreMemoryJUnitTest {

  @Override
  protected Properties getDistributedSystemProperties() {
    Properties dsProps = super.getDistributedSystemProperties();
    dsProps.setProperty(OFF_HEAP_MEMORY_SIZE, "1m");
    return dsProps;
  }

  @SuppressWarnings({"rawtypes", "deprecation"})
  @Override
  protected RegionFactory<?, ?> defineRegionFactory() {
    return new RegionFactory().setPartitionAttributes(definePartitionAttributes()).setOffHeap(true);
  }
}
