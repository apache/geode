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
