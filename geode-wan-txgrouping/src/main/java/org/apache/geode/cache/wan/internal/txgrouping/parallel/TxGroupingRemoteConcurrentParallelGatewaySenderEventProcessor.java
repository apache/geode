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

package org.apache.geode.cache.wan.internal.txgrouping.parallel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.wan.internal.parallel.RemoteConcurrentParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public class TxGroupingRemoteConcurrentParallelGatewaySenderEventProcessor extends
    RemoteConcurrentParallelGatewaySenderEventProcessor {

  public TxGroupingRemoteConcurrentParallelGatewaySenderEventProcessor(
      final @NotNull AbstractGatewaySender sender,
      final @Nullable ThreadsMonitoring threadsMonitoring, final boolean cleanQueues) {
    super(sender, threadsMonitoring, cleanQueues);
  }

  @Override
  protected ParallelGatewaySenderEventProcessor createRemoteParallelGatewaySenderEventProcessor(
      final @NotNull AbstractGatewaySender sender, final int id, final int dispatcherThreads,
      final @Nullable ThreadsMonitoring threadsMonitoring, final boolean cleanQueues) {
    return new TxGroupingRemoteParallelGatewaySenderEventProcessor(sender, id, dispatcherThreads,
        threadsMonitoring, cleanQueues);
  }
}
