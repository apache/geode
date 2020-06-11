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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Test;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;

// TODO: Remove the deprecated method invocations once RegionAttributesCreation is also updated.
public class CreateRegionFunctionTest {

  private RegionAttributes getRegionAttributesWithModifiedDiskDirs(final File[] diskDirs) {
    final RegionAttributes mockRegionAttributes = mock(RegionAttributes.class);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(null);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.NORMAL);
    when(mockRegionAttributes.getCacheListeners()).thenReturn(new CacheListener[] {});
    when(mockRegionAttributes.getEvictionAttributes()).thenReturn(EvictionAttributes
        .createLRUEntryAttributes(10, EvictionAction.OVERFLOW_TO_DISK));
    when(mockRegionAttributes.getDiskDirs()).thenReturn(diskDirs);

    return mockRegionAttributes;
  }

  private RegionAttributes getRegionAttributesWithModifiedDiskDirSize(final int[] diskDirSize) {
    final RegionAttributes mockRegionAttributes = mock(RegionAttributes.class);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(null);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.NORMAL);
    when(mockRegionAttributes.getCacheListeners()).thenReturn(new CacheListener[] {});
    when(mockRegionAttributes.getEvictionAttributes()).thenReturn(EvictionAttributes
        .createLRUEntryAttributes(10, EvictionAction.OVERFLOW_TO_DISK));

    when(mockRegionAttributes.getDiskDirSizes()).thenReturn(diskDirSize);

    return mockRegionAttributes;
  }

  @Test
  public void regionComparisonMustBeSuccessfulWhenDiskStoreNamesForBothAreNullAndDiskPropertiesAreDifferent() {
    final CreateRegionFunction createRegionFunction = mock(CreateRegionFunction.class);
    doCallRealMethod().when(createRegionFunction).compareRegionAttributes(any(), any());

    final RegionAttributes existingRegionAttributes =
        getRegionAttributesWithModifiedDiskDirSize(new int[] {1});
    final RegionAttributes requestedRegionAttributes =
        getRegionAttributesWithModifiedDiskDirSize(new int[] {2});
    createRegionFunction.compareRegionAttributes(existingRegionAttributes,
        requestedRegionAttributes);

    final RegionAttributes existingRegionAttributes2 =
        getRegionAttributesWithModifiedDiskDirs(null);
    final RegionAttributes requestedRegionAttributes2 =
        getRegionAttributesWithModifiedDiskDirs(new File[] {});
    createRegionFunction.compareRegionAttributes(existingRegionAttributes2,
        requestedRegionAttributes2);
  }
}
