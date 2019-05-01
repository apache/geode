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
package org.apache.geode.internal.statistics;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;


/**
 * Unit tests for {@link CallbackSampler}.
 */

public class CallbackSamplerTest {
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Mock
  StatSamplerStats statSamplerStats;

  @Mock
  StatisticsManager statisticsManager;

  private CallbackSampler sampler;

  @Before
  public void createSampler() {
    sampler = new CallbackSampler(statisticsManager, statSamplerStats);
  }

  @Test
  public void taskShouldSampleStatistics() {
    StatisticsImpl stats1 = mock(StatisticsImpl.class);
    StatisticsImpl stats2 = mock(StatisticsImpl.class);
    when(stats1.invokeSuppliers()).thenReturn(3);
    when(stats2.invokeSuppliers()).thenReturn(2);
    when(stats1.getSupplierCount()).thenReturn(7);
    when(stats2.getSupplierCount()).thenReturn(8);
    when(statisticsManager.getStatsList()).thenReturn(Arrays.asList(stats1, stats2));

    sampler.sampleCallbacks();

    verify(statSamplerStats).setSampleCallbacks(eq(15));
    verify(statSamplerStats).incSampleCallbackErrors(5);
    verify(statSamplerStats).incSampleCallbackDuration(anyLong());
  }

}
