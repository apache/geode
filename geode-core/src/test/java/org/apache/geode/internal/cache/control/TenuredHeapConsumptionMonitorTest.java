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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
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

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class TenuredHeapConsumptionMonitorTest {

  private BiConsumer<String, Throwable> infoLogger;
  private Function<CompositeData, MemoryNotificationInfo> memoryNotificationInfoFactory;
  private Notification notification;


  @Before
  public void setUp() {
    infoLogger = mock(BiConsumer.class);
    memoryNotificationInfoFactory = mock(Function.class);


  }

  private void notificationType(String type) {
    notification = new Notification(type, this, 1);
    ClientCache clientCache;
    clientCache = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    Pool defPool = gfc.getDefaultPool();
    MemoryUsage usage = new MemoryUsage(50, 50, 50, 100);
    MemoryNotificationInfo memoryNotificationInfo =
        new MemoryNotificationInfo(defPool.getName(), usage, 1);
    when(memoryNotificationInfoFactory.apply(any())).thenReturn(memoryNotificationInfo);
    TenuredHeapConsumptionMonitor monitor =
        new TenuredHeapConsumptionMonitor(infoLogger, memoryNotificationInfoFactory);

    monitor.checkTenuredHeapConsumption(notification);

  }

  @Test
  public void assertIfTenuredGCLogMessageIsPrintedAfterGCAndWhenMemoryThresholdExceeds() {
    notificationType(MEMORY_THRESHOLD_EXCEEDED);
    verify(infoLogger).accept(
        eq("A tenured heap garbage collection has occurred.  New tenured heap consumption: 50"),
        isNull());


  }

  @Test
  public void assertIfTenuredGCLogMessageIsPrintedAfterGCAndWhenMemoryCollectionThresholdExceeds() {
    notificationType(MEMORY_COLLECTION_THRESHOLD_EXCEEDED);
    verify(infoLogger).accept(
        eq("A tenured heap garbage collection has occurred.  New tenured heap consumption: 50"),
        isNull());


  }
}
