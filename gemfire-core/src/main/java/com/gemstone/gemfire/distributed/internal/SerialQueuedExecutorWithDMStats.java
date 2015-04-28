/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
