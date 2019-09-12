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

public class LonerOperationExecutors implements OperationExecutors {
  private final ExecutorService executor;

  public LonerOperationExecutors(ExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public Executor getExecutor(int processorType, InternalDistributedMember sender) {
    return executor;
  }

  @Override
  public ExecutorService getThreadPool() {
    return executor;
  }

  @Override
  public ExecutorService getHighPriorityThreadPool() {
    return executor;
  }

  @Override
  public ExecutorService getWaitingThreadPool() {
    return executor;
  }

  @Override
  public ExecutorService getPrMetaDataCleanupThreadPool() {
    return executor;
  }

  @Override
  public Executor getFunctionExecutor() {
    return executor;
  }

  @Override
  public OverflowQueueWithDMStats<Runnable> getSerialQueue(InternalDistributedMember sender) {
    throw new UnsupportedOperationException(
        "non-clustered caches do not support a serial executor queue");
  }
}
