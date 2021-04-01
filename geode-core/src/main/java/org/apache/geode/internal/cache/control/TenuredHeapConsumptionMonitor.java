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

import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryUsage;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.management.Notification;
import javax.management.openmbean.CompositeData;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;


class TenuredHeapConsumptionMonitor {

  private static final Logger logger = LogService.getLogger();
  private final BiConsumer<String, Throwable> infoLogger;
  private final Function<CompositeData, MemoryNotificationInfo> memoryNotificationInfoFactory;

  TenuredHeapConsumptionMonitor() {
    this(logger::info, MemoryNotificationInfo::from);
  }

  @VisibleForTesting
  TenuredHeapConsumptionMonitor(BiConsumer<String, Throwable> infoLogger,
      Function<CompositeData, MemoryNotificationInfo> memoryNotificationInfoFactory) {
    this.infoLogger = infoLogger;
    this.memoryNotificationInfoFactory = memoryNotificationInfoFactory;
  }

  void checkTenuredHeapConsumption(Notification notification) {
    try {
      String type = notification.getType();
      if (type.equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED) ||
          type.equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        // retrieve the memory notification information
        CompositeData compositeData = (CompositeData) notification.getUserData();
        MemoryNotificationInfo info = memoryNotificationInfoFactory.apply(compositeData);
        MemoryUsage usage = info.getUsage();
        long usedBytes = usage.getUsed();
        infoLogger.accept(
            "A tenured heap garbage collection has occurred.  New tenured heap consumption: "
                +
                usedBytes,
            null);
      }
    } catch (Exception e) {
      infoLogger.accept(
          "An Exception occurred while attempting to log tenured heap consumption", e);
    }
  }
}
