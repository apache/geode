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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionAttributes;

public class ColocationHelperTest {

  private CancelCriterion cancelCriterion;
  private PartitionedRegion partitionedRegion;
  private PartitionRegionConfig partitionRegionConfig;
  private DistributedRegion prRoot;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);

    cancelCriterion = mock(CancelCriterion.class);
    partitionRegionConfig = mock(PartitionRegionConfig.class);
    partitionedRegion = mock(PartitionedRegion.class);
    prRoot = mock(DistributedRegion.class);

    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cache.getRegion(anyString(), anyBoolean())).thenReturn(prRoot);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(partitionAttributes);

    when(partitionAttributes.getColocatedWith()).thenReturn("region2");
  }

  @Test
  public void getColocatedRegion_throwsIllegalStateException_ifNotColocated() {
    when(partitionedRegion.getFullPath()).thenReturn("/region1");

    Throwable thrown = catchThrowable(() -> ColocationHelper.getColocatedRegion(partitionedRegion));

    assertThat(thrown)
        .as("Expected IllegalStateException for missing colocated parent region")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageMatching("Region specified in 'colocated-with' .* does not exist.*");
  }

  @Test
  public void getColocatedRegion_logsWarning_ifMissingRegion_whenPRConfigHasRegion() {
    when(partitionedRegion.getFullPath()).thenReturn("/region1");
    when(prRoot.get(eq("#region2"))).thenReturn(partitionRegionConfig);

    Throwable thrown = catchThrowable(() -> ColocationHelper.getColocatedRegion(partitionedRegion));

    assertThat(thrown)
        .as("Expected IllegalStateException for missing colocated parent region")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageMatching("Region specified in 'colocated-with' .* does not exist.*");
  }

  @Test
  public void getColocatedRegion_throwsCacheClosedException_whenCacheIsClosed() {
    doThrow(new CacheClosedException("test")).when(cancelCriterion).checkCancelInProgress(any());
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);

    Throwable thrown = catchThrowable(() -> ColocationHelper.getColocatedRegion(partitionedRegion));

    assertThat(thrown)
        .isInstanceOf(CacheClosedException.class);
  }
}
