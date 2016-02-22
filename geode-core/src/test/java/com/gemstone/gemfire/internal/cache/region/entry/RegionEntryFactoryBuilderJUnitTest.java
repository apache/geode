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
package com.gemstone.gemfire.internal.cache.region.entry;

import com.gemstone.gemfire.test.junit.categories.UnitTest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
public class RegionEntryFactoryBuilderJUnitTest {

  private RegionEntryFactoryBuilder regionEntryFactoryBuilder;

  @Before
  public void setup() {
    regionEntryFactoryBuilder = new RegionEntryFactoryBuilder();
  }

  /**
   * This method will test that the correct RegionEntryFactory is created
   * dependent on the 5 conditionals:
   * enableStats, enableLRU, enableDisk, enableVersion, enableOffHeap
   */
  @Test
  @Parameters({
      "VMThinRegionEntryHeapFactory,false,false,false,false,false",
      "VMThinRegionEntryOffHeapFactory,false,false,false,false,true",
      "VersionedThinRegionEntryHeapFactory,false,false,false,true,false",
      "VersionedThinRegionEntryOffHeapFactory,false,false,false,true,true",
      "VMThinDiskRegionEntryHeapFactory,false,false,true,false,false",
      "VMThinDiskRegionEntryOffHeapFactory,false,false,true,false,true",
      "VersionedThinDiskRegionEntryHeapFactory,false,false,true,true,false",
      "VersionedThinDiskRegionEntryOffHeapFactory,false,false,true,true,true",
      "VMThinLRURegionEntryHeapFactory,false,true,false,false,false",
      "VMThinLRURegionEntryOffHeapFactory,false,true,false,false,true",
      "VersionedThinLRURegionEntryHeapFactory,false,true,false,true,false",
      "VersionedThinLRURegionEntryOffHeapFactory,false,true,false,true,true",
      "VMThinDiskLRURegionEntryHeapFactory,false,true,true,false,false",
      "VMThinDiskLRURegionEntryOffHeapFactory,false,true,true,false,true",
      "VersionedThinDiskLRURegionEntryHeapFactory,false,true,true,true,false",
      "VersionedThinDiskLRURegionEntryOffHeapFactory,false,true,true,true,true",
      "VMStatsRegionEntryHeapFactory,true,false,false,false,false",
      "VMStatsRegionEntryOffHeapFactory,true,false,false,false,true",
      "VersionedStatsRegionEntryHeapFactory,true,false,false,true,false",
      "VersionedStatsRegionEntryOffHeapFactory,true,false,false,true,true",
      "VMStatsDiskRegionEntryHeapFactory,true,false,true,false,false",
      "VMStatsDiskRegionEntryOffHeapFactory,true,false,true,false,true",
      "VersionedStatsDiskRegionEntryHeapFactory,true,false,true,true,false",
      "VersionedStatsDiskRegionEntryOffHeapFactory,true,false,true,true,true",
      "VMStatsLRURegionEntryHeapFactory,true,true,false,false,false",
      "VMStatsLRURegionEntryOffHeapFactory,true,true,false,false,true",
      "VersionedStatsLRURegionEntryHeapFactory,true,true,false,true,false",
      "VersionedStatsLRURegionEntryOffHeapFactory,true,true,false,true,true",
      "VMStatsDiskLRURegionEntryHeapFactory,true,true,true,false,false",
      "VMStatsDiskLRURegionEntryOffHeapFactory,true,true,true,false,true",
      "VersionedStatsDiskLRURegionEntryHeapFactory,true,true,true,true,false",
      "VersionedStatsDiskLRURegionEntryOffHeapFactory,true,true,true,true,true"
  })
  public void testRegionEntryFactoryUnitTest(String factoryName, boolean enableStats, boolean enableLRU, boolean enableDisk,
      boolean enableVersioning, boolean enableOffHeap) {
    assertEquals(factoryName,
        regionEntryFactoryBuilder.getRegionEntryFactoryOrNull(enableStats, enableLRU, enableDisk, enableVersioning, enableOffHeap).getClass().getSimpleName());
  }
}