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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;

public class AbstractGatewaySenderEventProcessorIntegrationTest {

  protected InternalCache cache;

  @Before
  public void setUp() {
    this.cache = (InternalCache) new CacheFactory().create();
  }

  @After
  public void tearDown() {
    if (this.cache != null) {
      this.cache.close();
    }
  }

  @Test
  public void verifyThresholdExceededAlertLogDoesNotThrowException() {
    // Mock the sender
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    when(sender.getAlertThreshold()).thenReturn(1);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));

    // Mock the processor
    AbstractGatewaySenderEventProcessor eventProcessor =
        mock(AbstractGatewaySenderEventProcessor.class);
    when(eventProcessor.getSender()).thenReturn(sender);

    // Mock the region
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getCache()).thenReturn(this.cache);

    // Create the events
    List<GatewaySenderEventImpl> events = new ArrayList<>();
    GatewaySenderEventImpl gsei1 = mock(GatewaySenderEventImpl.class);
    when(gsei1.getValueAsString(true)).thenThrow(new IllegalStateException("test"));
    events.add(gsei1);

    // Invoke the real method
    doCallRealMethod().when(eventProcessor).logThresholdExceededAlerts(events);
    assertThatCode(() -> eventProcessor.logThresholdExceededAlerts(events))
        .doesNotThrowAnyException();
  }
}
