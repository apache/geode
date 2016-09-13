/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is used for the DM's serial executor.
 * The only thing it currently does is increment stats.
 */
public class SerialQueuedExecutorWithDMStats extends ThreadPoolExecutor  {
  final PoolStatHelper stats;
  
  public SerialQueuedExecutorWithDMStats(BlockingQueue q,
                       PoolStatHelper stats,
                       ThreadFactory tf) {
    super(1, 1, 60, TimeUnit.SECONDS, q, tf, new PooledExecutorWithDMStats.BlockHandler());
    //allowCoreThreadTimeOut(true); // deadcoded for 1.5
    this.stats = stats;
  }
  @Override
  protected final void beforeExecute(Thread t, Runnable r) {
    if (this.stats != null) {
      this.stats.startJob();
    }
  }

  @Override
  protected final void afterExecute(Runnable r, Throwable ex) {
    if (this.stats != null) {
      this.stats.endJob();
    }
  }
}
