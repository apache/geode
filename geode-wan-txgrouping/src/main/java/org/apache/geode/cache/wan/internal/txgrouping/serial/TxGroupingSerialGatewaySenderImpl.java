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

package org.apache.geode.cache.wan.internal.txgrouping.serial;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.wan.internal.serial.SerialGatewaySenderImpl;
import org.apache.geode.cache.wan.internal.txgrouping.TxGroupingGatewaySender;
import org.apache.geode.cache.wan.internal.txgrouping.TxGroupingGatewaySenderProperties;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.statistics.StatisticsClock;

public class TxGroupingSerialGatewaySenderImpl extends SerialGatewaySenderImpl
    implements TxGroupingGatewaySender {

  public static final String TYPE = "TxGroupingSerialGatewaySender";

  private final TxGroupingGatewaySenderProperties properties =
      new TxGroupingGatewaySenderProperties();

  public TxGroupingSerialGatewaySenderImpl(final @NotNull InternalCache cache,
      final @NotNull StatisticsClock clock,
      final @NotNull GatewaySenderAttributes attributes) {
    super(cache, clock, attributes);
  }

  @Override
  public boolean mustGroupTransactionEvents() {
    return true;
  }

  @Override
  protected AbstractGatewaySenderEventProcessor createEventProcessor(
      final @Nullable ThreadsMonitoring threadsMonitoring, final boolean cleanQueues) {
    if (getDispatcherThreads() > 1) {
      return new TxGroupingRemoteConcurrentSerialGatewaySenderEventProcessor(this,
          threadsMonitoring, cleanQueues);
    } else {
      return new TxGroupingRemoteSerialGatewaySenderEventProcessor(this, getId(),
          threadsMonitoring, cleanQueues);
    }
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void setRetriesToGetTransactionEventsFromQueue(int retries) {
    properties.setRetriesToGetTransactionEventsFromQueue(retries);
  }

  @Override
  public int getRetriesToGetTransactionEventsFromQueue() {
    return properties.getRetriesToGetTransactionEventsFromQueue();
  }

  @Override
  public void setTransactionEventsFromQueueWaitMs(int millisecs) {
    properties.setTransactionEventsFromQueueWaitMs(millisecs);
  }

  @Override
  public int getTransactionEventsFromQueueWaitMs() {
    return properties.getTransactionEventsFromQueueWaitMs();
  }

  @Override
  public boolean isParallel() {
    return false;
  }
}
