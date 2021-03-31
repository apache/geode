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

import static java.lang.management.MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED;
import static java.lang.management.MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryUsage;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.management.Notification;
import javax.management.openmbean.CompositeData;

import org.junit.Before;
import org.junit.Test;

public class TenuredHeapConsumptionMonitorTest {

  private BiConsumer<String, Throwable> infoLogger;
  private Function<CompositeData, MemoryNotificationInfo> memoryNotificationInfoFactory;
  private Notification notification;
  private final long used = 50;
  private TenuredHeapConsumptionMonitor monitor;

  @Before
  public void setUp() {
    notification = mock(Notification.class);
    MemoryUsage usage = mock(MemoryUsage.class);
    MemoryNotificationInfo memoryNotificationInfo = mock(MemoryNotificationInfo.class);
    when(memoryNotificationInfo.getUsage()).thenReturn(usage);
    when(usage.getUsed()).thenReturn(used);
    memoryNotificationInfoFactory = mock(Function.class);
    when(memoryNotificationInfoFactory.apply(any())).thenReturn(memoryNotificationInfo);
    infoLogger = mock(BiConsumer.class);
    monitor = new TenuredHeapConsumptionMonitor(infoLogger, memoryNotificationInfoFactory);

  }

  @Test
  public void assertIfTenuredGCLogMessageIsPrintedAfterGCAndWhenMemoryThresholdExceeds() {
    when(notification.getType()).thenReturn(MEMORY_THRESHOLD_EXCEEDED);
    monitor.checkTenuredHeapConsumption(notification);
    verify(infoLogger).accept(
        eq("A tenured heap garbage collection has occurred.  New tenured heap consumption: "
            + used),
        isNull());
  }

  @Test
  public void assertIfTenuredGCLogMessageIsPrintedAfterGCAndWhenMemoryCollectionThresholdExceeds() {
    when(notification.getType()).thenReturn(MEMORY_COLLECTION_THRESHOLD_EXCEEDED);
    monitor.checkTenuredHeapConsumption(notification);
    verify(infoLogger).accept(
        eq("A tenured heap garbage collection has occurred.  New tenured heap consumption: "
            + used),
        isNull());


  }

  @Test
  public void assertThatNothingIsLoggedWhenNotificationTypeIsNotMemoryThresholdExceededOrMemoryCollectionThresholdExceeded() {
    when(notification.getType()).thenReturn("FAKE_TYPE");
    monitor.checkTenuredHeapConsumption(notification);
    verify(infoLogger, never()).accept(anyString(), any());
  }

  @Test
  public void exceptionMessageIsLoggedWhenExceptionIsThrownInCheckTenuredHeapConsumption() {
    IllegalArgumentException ex = new IllegalArgumentException("test message");
    when(memoryNotificationInfoFactory.apply(any())).thenThrow(ex);
    when(notification.getType()).thenReturn(MEMORY_COLLECTION_THRESHOLD_EXCEEDED);
    monitor.checkTenuredHeapConsumption(notification);
    verify(infoLogger)
        .accept("An Exception occurred while attempting to log tenured heap consumption", ex);
  }
}
