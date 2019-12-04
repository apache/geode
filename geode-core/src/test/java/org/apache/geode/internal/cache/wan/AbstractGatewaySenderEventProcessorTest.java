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
package org.apache.geode.internal.cache.wan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.internal.cache.RegionQueue;

public class AbstractGatewaySenderEventProcessorTest {

  private RegionQueue queue = mock(RegionQueue.class);

  @Test
  public void eventQueueSizeReturnsQueueSize() {
    AbstractGatewaySenderEventProcessor processor = mock(AbstractGatewaySenderEventProcessor.class);
    when(processor.getQueue()).thenReturn(queue);
    doCallRealMethod().when(processor).eventQueueSize();

    processor.eventQueueSize();

    verify(queue).size();
  }

  @Test
  public void eventQueueSizeReturnsZeroIfRegionQueueIsNull() {
    AbstractGatewaySenderEventProcessor processor = mock(AbstractGatewaySenderEventProcessor.class);
    doCallRealMethod().when(processor).eventQueueSize();

    assertThat(processor.eventQueueSize()).isEqualTo(0);

    verify(queue, never()).size();
  }
}
