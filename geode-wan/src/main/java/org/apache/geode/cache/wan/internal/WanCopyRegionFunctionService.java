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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

public class WanCopyRegionFunctionService implements CacheService {

  private ExecutorService wanCopyRegionFunctionExecutionPool;

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

  public Executor getExecutor() {
    return wanCopyRegionFunctionExecutionPool;
  }
}
