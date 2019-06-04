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
package org.apache.geode.cache.client.internal;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.LocalRegion;

public interface SingleHopClientExecutor {
  void submitAll(List callableTasks);

  int submitAllHA(List callableTasks, LocalRegion region, boolean isHA,
      ResultCollector rc, Set<String> failedNodes, int retryAttemptsArg,
      PoolImpl pool);

  Map<ServerLocation, Object> submitBulkOp(List callableTasks, ClientMetadataService cms,
      LocalRegion region,
      Map<ServerLocation, RuntimeException> failedServers);

  Map<ServerLocation, Object> submitGetAll(Map<ServerLocation, HashSet> serverToFilterMap,
      List callableTasks, ClientMetadataService cms,
      LocalRegion region);

  void submitTask(Runnable task);
}
