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
package org.apache.geode.modules.util;

import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.InternalCache;

public class CreateRegionFunctionTest {

  private static final EvictionAttributes LRU_ENTRY_ATTRIBUTES =
      EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.OVERFLOW_TO_DISK);

  private CreateRegionFunction createRegionFunction;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    Region<String, RegionConfiguration> regionConfigurationsRegion = cast(mock(Region.class));

    createRegionFunction = spy(new CreateRegionFunction(cache, regionConfigurationsRegion,
        (cache1, config) -> null, (cache1, config, region) -> {
        }));
  }

  @Test
  public void regionComparisonMustBeSuccessfulWhenDiskStoreNamesForBothAreNullAndDiskPropertiesAreDifferent() {
    RegionAttributes<?, ?> existingRegionAttributes =
        regionAttributesWithModifiedDiskDirSize(new int[] {1});
    RegionAttributes<?, ?> requestedRegionAttributes =
        regionAttributesWithModifiedDiskDirSize(new int[] {2});

    createRegionFunction.compareRegionAttributes(existingRegionAttributes,
        requestedRegionAttributes);

    RegionAttributes<?, ?> existingRegionAttributes2 =
        regionAttributesWithModifiedDiskDirs(null);
    RegionAttributes<?, ?> requestedRegionAttributes2 =
        regionAttributesWithModifiedDiskDirs(new File[] {});

    createRegionFunction.compareRegionAttributes(existingRegionAttributes2,
        requestedRegionAttributes2);
  }

  private RegionAttributes<?, ?> regionAttributesWithModifiedDiskDirs(File[] diskDirs) {
    RegionAttributes<?, ?> regionAttributes = regionAttributes();
    when(regionAttributes.getDiskDirs()).thenReturn(diskDirs);
    return regionAttributes;
  }

  private RegionAttributes<?, ?> regionAttributesWithModifiedDiskDirSize(int[] diskDirSize) {
    RegionAttributes<?, ?> regionAttributes = regionAttributes();
    when(regionAttributes.getDiskDirSizes()).thenReturn(diskDirSize);
    return regionAttributes;
  }

  private RegionAttributes<?, ?> regionAttributes() {
    RegionAttributes regionAttributes = mock(RegionAttributes.class);

    when(regionAttributes.getCacheListeners()).thenReturn(new CacheListener[] {});
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.NORMAL);
    when(regionAttributes.getDiskStoreName()).thenReturn(null);
    when(regionAttributes.getEvictionAttributes()).thenReturn(LRU_ENTRY_ATTRIBUTES);

    return regionAttributes;
  }
}
