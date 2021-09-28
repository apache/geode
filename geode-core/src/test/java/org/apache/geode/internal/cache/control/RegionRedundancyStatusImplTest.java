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
package org.apache.geode.internal.cache.control;

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.NO_REDUNDANT_COPIES;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.SATISFIED;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class RegionRedundancyStatusImplTest {

  private PartitionedRegion mockRegion;
  private final int desiredRedundancy = 2;
  private final int oneRedundantCopy = 1;
  private final int zeroRedundancy = 0;

  @Before
  public void setUp() {
    mockRegion = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    when(mockRegion.getRedundantCopies()).thenReturn(desiredRedundancy);
    when(mockRegion.getPartitionAttributes().getTotalNumBuckets())
        .thenReturn(GLOBAL_MAX_BUCKETS_DEFAULT);
  }

  @Test
  @Parameters(method = "getActualRedundancyAndExpectedStatusAndMessage")
  @TestCaseName("[{index}] {method} (Desired redundancy:" + desiredRedundancy
      + "; Actual redundancy:{0}; Expected status:{1})")
  public void constructorPopulatesValuesCorrectlyWhenAllBucketsExist(int actualRedundancy,
      RegionRedundancyStatus.RedundancyStatus expectedStatus) {
    when(mockRegion.getRegionAdvisor().getBucketRedundancy(anyInt())).thenReturn(actualRedundancy);

    RegionRedundancyStatus result = new SerializableRegionRedundancyStatusImpl(mockRegion);

    assertThat(result.getConfiguredRedundancy(), is(desiredRedundancy));
    assertThat(result.getActualRedundancy(), is(actualRedundancy));
    assertThat(result.getStatus(), is(expectedStatus));
    assertThat(result.toString(), containsString(expectedStatus.name()));
  }

  @Test
  @Parameters(method = "getActualRedundancyAndExpectedStatusAndMessage")
  @TestCaseName("[{index}] {method} (Desired redundancy:" + desiredRedundancy
      + "; Actual redundancy:{0}; Expected status:{1})")
  public void constructorPopulatesValuesCorrectlyWhenNotAllBucketsExist(int actualRedundancy,
      RegionRedundancyStatus.RedundancyStatus expectedStatus) {
    when(mockRegion.getRegionAdvisor().getBucketRedundancy(anyInt())).thenReturn(-1)
        .thenReturn(actualRedundancy);

    RegionRedundancyStatus result = new SerializableRegionRedundancyStatusImpl(mockRegion);

    assertThat(result.getConfiguredRedundancy(), is(desiredRedundancy));
    assertThat(result.getActualRedundancy(), is(actualRedundancy));
    assertThat(result.getStatus(), is(expectedStatus));
    assertThat(result.toString(), containsString(expectedStatus.name()));
  }

  @Test
  public void constructorPopulatesValuesCorrectlyWhenNotAllBucketsReturnTheSameRedundancy() {
    when(mockRegion.getRegionAdvisor().getBucketRedundancy(anyInt())).thenReturn(desiredRedundancy);
    // Have only the bucket with ID = 1 report being under redundancy
    when(mockRegion.getRegionAdvisor().getBucketRedundancy(1)).thenReturn(oneRedundantCopy);

    RegionRedundancyStatus result = new SerializableRegionRedundancyStatusImpl(mockRegion);

    assertThat(result.getConfiguredRedundancy(), is(desiredRedundancy));
    assertThat(result.getActualRedundancy(), is(oneRedundantCopy));
    assertThat(result.getStatus(), is(NOT_SATISFIED));
    assertThat(result.toString(), containsString(NOT_SATISFIED.name()));
  }

  public Object[] getActualRedundancyAndExpectedStatusAndMessage() {
    return new Object[] {
        new Object[] {desiredRedundancy, SATISFIED},
        new Object[] {oneRedundantCopy, NOT_SATISFIED},
        new Object[] {zeroRedundancy, NO_REDUNDANT_COPIES}
    };
  }


}
