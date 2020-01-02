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

package org.apache.geode.internal.cache.tier.sockets;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.util.internal.GeodeGlossary;

public class TcpServerFactory {
  private static final int MAX_POOL_SIZE =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.MAX_POOL_SIZE", 100);
  private static final int POOL_IDLE_TIMEOUT = 60 * 1000;

  public TcpServerFactory() {}

  public static Supplier<ExecutorService> createExecutorServiceSupplier(PoolStatHelper poolHelper) {
    return () -> CoreLoggingExecutors.newThreadPoolWithSynchronousFeed("locator request thread ",
        MAX_POOL_SIZE, poolHelper, POOL_IDLE_TIMEOUT, new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
