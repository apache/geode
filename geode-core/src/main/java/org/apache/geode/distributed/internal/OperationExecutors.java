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
package org.apache.geode.distributed.internal;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * OperationExecutors holds the thread pools used to execute cache operations. There
 * are a number of different thread pools available. Each DistributionMessage has
 * a processor-type that determines which pool will be used to execute the message.
 */
public interface OperationExecutors {
  int MAX_THREADS =
      Integer.getInteger("DistributionManager.MAX_THREADS", 100);

  int MAX_FE_THREADS = Integer.getInteger("DistributionManager.MAX_FE_THREADS",
      Math.max(Runtime.getRuntime().availableProcessors() * 4, 16));
  /**
   * @see PooledDistributionMessage
   */
  int STANDARD_EXECUTOR = 73;
  /**
   * @see SerialDistributionMessage
   */
  int SERIAL_EXECUTOR = 74;
  /**
   * @see HighPriorityDistributionMessage
   */
  int HIGH_PRIORITY_EXECUTOR = 75;
  /**
   * @see org.apache.geode.internal.cache.InitialImageOperation
   */
  int WAITING_POOL_EXECUTOR = 77;
  /**
   * @see org.apache.geode.internal.cache.InitialImageOperation
   */
  int PARTITIONED_REGION_EXECUTOR = 78;

  int REGION_FUNCTION_EXECUTION_EXECUTOR = 80;

  String FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX =
      "Function Execution Processor";

  Executor getExecutor(int processorType, InternalDistributedMember sender);

  ExecutorService getThreadPool();

  ExecutorService getHighPriorityThreadPool();

  ExecutorService getWaitingThreadPool();

  ExecutorService getPrMetaDataCleanupThreadPool();

  Executor getFunctionExecutor();

  OverflowQueueWithDMStats<Runnable> getSerialQueue(InternalDistributedMember sender);
}
