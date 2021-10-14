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

package org.apache.geode.cache.wan.internal;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class WanCopyRegionFunctionService implements CacheService {

  private volatile ExecutorService wanCopyRegionFunctionExecutionPool;

  /**
   * Contains the ongoing executions of WanCopyRegionFunction
   */
  private final Map<String, Future<CliFunctionResult>> executions =
      new ConcurrentHashMap<>();

  @Override
  public boolean init(Cache cache) {
    String WAN_COPY_REGION_FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX =
        "WAN Copy Region Function Execution Processor";
    int WAN_COPY_REGION_FUNCTION_MAX_CONCURRENT_THREADS = 10;
    wanCopyRegionFunctionExecutionPool = LoggingExecutors
        .newFixedThreadPool(WAN_COPY_REGION_FUNCTION_MAX_CONCURRENT_THREADS,
            WAN_COPY_REGION_FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX, true);
    return true;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return WanCopyRegionFunctionService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }

  @Override
  public void close() {
    wanCopyRegionFunctionExecutionPool.shutdownNow();
    try {
      if (!wanCopyRegionFunctionExecutionPool.awaitTermination(5, TimeUnit.SECONDS)) {
        wanCopyRegionFunctionExecutionPool.shutdownNow();
      }
    } catch (InterruptedException ie) {
      wanCopyRegionFunctionExecutionPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public CliFunctionResult execute(Callable<CliFunctionResult> callable,
      String regionName, String senderId) throws InterruptedException, ExecutionException,
      WanCopyRegionFunctionServiceAlreadyRunningException {
    String executionName = getExecutionName(regionName, senderId);
    Future<CliFunctionResult> future = null;
    try {
      synchronized (executions) {
        if (executions.containsKey(executionName)) {
          throw new WanCopyRegionFunctionServiceAlreadyRunningException(
              "There is already an execution running for " + regionName + " and " + senderId);
        }
        future = wanCopyRegionFunctionExecutionPool.submit(callable);
        executions.put(executionName, future);
      }
      return future.get();
    } finally {
      if (future != null) {
        executions.remove(executionName);
      }
    }
  }

  public boolean cancel(String regionName, String senderId) {
    Future<CliFunctionResult> execution = executions.remove(getExecutionName(regionName, senderId));
    if (execution == null) {
      return false;
    }
    execution.cancel(true);
    return true;
  }

  public String cancelAll() {
    String executionsString = executions.keySet().toString();
    for (Future<CliFunctionResult> execution : executions.values()) {
      execution.cancel(true);
    }
    executions.clear();
    return executionsString;
  }

  public int getNumberOfCurrentExecutions() {
    return executions.size();
  }

  private String getExecutionName(String regionName, String senderId) {
    return "(" + regionName + "," + senderId + ")";
  }
}
