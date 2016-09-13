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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.*;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DistributedRegionJUnitTest extends AbstractDistributedRegionJUnitTest {

  @Override
  protected void setInternalRegionArguments(InternalRegionArguments ira) {
  }

  @Override
  protected DistributedRegion createAndDefineRegion(boolean isConcurrencyChecksEnabled,
      RegionAttributes ra, InternalRegionArguments ira, GemFireCacheImpl cache) {
    DistributedRegion region = new DistributedRegion("testRegion", ra, null, cache, ira);
    if (isConcurrencyChecksEnabled) {
      region.enableConcurrencyChecks();
    }
    
    // since it is a real region object, we need to tell mockito to monitor it
    region = spy(region);

    doNothing().when(region).distributeUpdate(any(), anyLong(), anyBoolean(), anyBoolean(), any(), anyBoolean());
    doNothing().when(region).distributeDestroy(any(), any());
    doNothing().when(region).distributeInvalidate(any());
    doNothing().when(region).distributeUpdateEntryVersion(any());
    
    return region;
  }

  @Override
  protected void verifyDistributeUpdate(DistributedRegion region, EntryEventImpl event, int cnt) {
    region.virtualPut(event, false, false, null, false, 12345L, false);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeUpdate(eq(event), eq(12345L), anyBoolean(), anyBoolean(), any(), anyBoolean());
    } else {
      verify(region, never()).distributeUpdate(eq(event), eq(12345L), anyBoolean(), anyBoolean(), any(), anyBoolean());
    }
  }

  @Override
  protected void verifyDistributeDestroy(DistributedRegion region, EntryEventImpl event, int cnt) {
    region.basicDestroy(event, false, null);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeDestroy(eq(event), any());
    } else {
      verify(region, never()).distributeDestroy(eq(event), any());
    }
  }

  @Override
  protected void verifyDistributeInvalidate(DistributedRegion region, EntryEventImpl event, int cnt) {
    region.basicInvalidate(event);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeInvalidate(eq(event));
    } else {
      verify(region, never()).distributeInvalidate(eq(event));
    }
  }

  @Override
  protected void verifyDistributeUpdateEntryVersion(DistributedRegion region, EntryEventImpl event, int cnt) {
    region.basicUpdateEntryVersion(event);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeUpdateEntryVersion(eq(event));
    } else {
      verify(region, never()).distributeUpdateEntryVersion(eq(event));
    }
  }
  
}

