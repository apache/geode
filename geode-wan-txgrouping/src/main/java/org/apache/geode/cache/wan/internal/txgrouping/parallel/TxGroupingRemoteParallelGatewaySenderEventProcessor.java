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

import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.internal.parallel.RemoteParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public class TxGroupingRemoteParallelGatewaySenderEventProcessor extends
    RemoteParallelGatewaySenderEventProcessor {

  protected TxGroupingRemoteParallelGatewaySenderEventProcessor(
      final @NotNull AbstractGatewaySender sender, final int id, final int nDispatcher,
      final @Nullable ThreadsMonitoring threadsMonitoring, final boolean cleanQueues) {
    super(sender, id, nDispatcher, threadsMonitoring, cleanQueues);
  }


  @Override
  protected @NotNull ParallelGatewaySenderQueue createParallelGatewaySenderQueue(
      final @NotNull AbstractGatewaySender sender, final @NotNull Set<Region<?, ?>> targetRegions,
      final int index, final int dispatcherThreads, final boolean cleanQueues) {
    return new TxGroupingParallelGatewaySenderQueue(sender, targetRegions, index, dispatcherThreads,
        cleanQueues);
  }
}
