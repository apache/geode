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
package org.apache.geode.internal.cache.wan.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public class ParallelGatewaySenderEventProcessorTest {

  private final AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
  private final ParallelGatewaySenderQueue queue = mock(ParallelGatewaySenderQueue.class);

  @Before
  public void setup() {
    when(sender.getCache()).thenReturn(mock(InternalCache.class));
    when(sender.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(sender.getId()).thenReturn("");
  }

  @Test
  public void eventQueueSizeReturnsQueueLocalSize() {
    ParallelGatewaySenderEventProcessor processor =
        spy(new ParallelGatewaySenderEventProcessor(sender, mock(
            ThreadsMonitoring.class), false));
    doReturn(queue).when(processor).getQueue();

    processor.eventQueueSize();

    verify(queue).localSize();
  }

  @Test
  public void eventQueueSizeReturnsZeroIfQueueIsNull() {
    ParallelGatewaySenderEventProcessor processor =
        spy(new ParallelGatewaySenderEventProcessor(sender, mock(
            ThreadsMonitoring.class), false));
    doReturn(null).when(processor).getQueue();

    assertThat(processor.eventQueueSize()).isEqualTo(0);

    verify(queue, never()).localSize();
  }
}
