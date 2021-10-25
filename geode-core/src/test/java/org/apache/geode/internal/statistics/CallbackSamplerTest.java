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

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.apache.geode.CancelCriterion;

/**
 * Unit tests for {@link CallbackSampler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class CallbackSamplerTest {

  @Mock
  CancelCriterion cancelCriterion;
  @Mock
  StatSamplerStats statSamplerStats;
  @Mock
  StatisticsManager statisticsManager;
  @Mock
  ScheduledExecutorService executorService;

  private CallbackSampler sampler;

  @Before
  public void createSampler() {
    sampler = new CallbackSampler(cancelCriterion, statSamplerStats);
  }

  @Test
  public void taskShouldSampleStatistics() {
    Runnable sampleTask = invokeStartAndGetTask();

    StatisticsImpl stats1 = mock(StatisticsImpl.class);
    StatisticsImpl stats2 = mock(StatisticsImpl.class);
    when(stats1.updateSuppliedValues()).thenReturn(3);
    when(stats2.updateSuppliedValues()).thenReturn(2);
    when(stats1.getSupplierCount()).thenReturn(7);
    when(stats2.getSupplierCount()).thenReturn(8);
    when(statisticsManager.getStatsList()).thenReturn(Arrays.asList(stats1, stats2));
    sampleTask.run();
    verify(statSamplerStats).setSampleCallbacks(eq(15));
    verify(statSamplerStats).incSampleCallbackErrors(5);
    verify(statSamplerStats).incSampleCallbackDuration(anyLong());
  }

  @Test
  public void stopShouldStopExecutor() {
    sampler.start(executorService, statisticsManager, 1, TimeUnit.MILLISECONDS);
    sampler.stop();
    verify(executorService).shutdown();
  }

  @Test
  public void cancelCriterionShouldStopExecutor() {
    Runnable sampleTask = invokeStartAndGetTask();
    when(cancelCriterion.isCancelInProgress()).thenReturn(Boolean.TRUE);
    sampleTask.run();
    verify(executorService).shutdown();
  }

  private Runnable invokeStartAndGetTask() {
    sampler.start(executorService, statisticsManager, 1, TimeUnit.MILLISECONDS);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(executorService).scheduleAtFixedRate(runnableCaptor.capture(), eq(1L), eq(1L),
        eq(TimeUnit.MILLISECONDS));
    return runnableCaptor.getValue();
  }

}
