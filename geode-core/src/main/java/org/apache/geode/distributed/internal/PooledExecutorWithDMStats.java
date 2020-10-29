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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * A ThreadPoolExecutor with stat support.
 */
public class PooledExecutorWithDMStats extends ThreadPoolExecutor {

  private final PoolStatHelper poolStatHelper;

  private final ThreadsMonitoring threadsMonitoring;

  /**
   * Used to buffer up tasks that would be have been rejected. Only used (i.e. non-null) if
   * constructor queue is not a SynchronousQueue.
   */
  private BlockingQueue<Runnable> blockingWorkQueue;

  /**
   * Used to consume items off the bufferQueue and put them into the pools synchronous queue. Only
   * used (i.e. non-null) if constructor queue is not a SynchronousQueue.
   */
  private Thread bufferConsumer;

  /**
   * Create a new pool that uses the supplied Channel for queuing, and with all default parameter
   * settings except for pool size.
   **/
  public PooledExecutorWithDMStats(int poolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, PoolStatHelper poolStatHelper,
      ThreadsMonitoring threadsMonitoring) {
    this(poolSize, keepAliveTime, unit, getSynchronousQueue(workQueue), threadFactory,
        newRejectedExecutionHandler(workQueue), poolStatHelper, threadsMonitoring);

    if (!(workQueue instanceof SynchronousQueue)) {
      blockingWorkQueue = workQueue;
      // create a thread that takes from bufferQueue and puts into result
      final BlockingQueue<Runnable> takeQueue = workQueue;
      final BlockingQueue<Runnable> putQueue = getQueue();
      Runnable r = () -> {
        try {
          for (;;) {
            SystemFailure.checkFailure();
            Runnable job = takeQueue.take();
            putQueue.put(job);
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          // this thread is being shutdown so just return;
        }
      };
      bufferConsumer = threadFactory.newThread(r);
      bufferConsumer.start();
    }
  }

  /**
   * Create a new pool
   */
  public PooledExecutorWithDMStats(int poolSize, long keepAliveTime, TimeUnit unit,
      SynchronousQueue<Runnable> workQueue, ThreadFactory threadFactory,
      RejectedExecutionHandler rejectedExecutionHandler, PoolStatHelper poolStatHelper,
      ThreadsMonitoring threadsMonitoring) {
    super(getCorePoolSize(poolSize), poolSize, keepAliveTime, unit, workQueue, threadFactory,
        rejectedExecutionHandler);

    this.poolStatHelper = poolStatHelper;
    this.threadsMonitoring = threadsMonitoring;
  }

  @Override
  public void shutdown() {
    try {
      super.shutdown();
    } finally {
      terminated();
    }
  }

  @Override
  protected void terminated() {
    if (bufferConsumer != null) {
      bufferConsumer.interrupt();
    }
    super.terminated();
  }

  @Override
  public List<Runnable> shutdownNow() {
    terminated();
    List<Runnable> l = super.shutdownNow();
    if (blockingWorkQueue != null) {
      blockingWorkQueue.drainTo(l);
    }
    return l;
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (poolStatHelper != null) {
      poolStatHelper.startJob();
    }
    if (threadsMonitoring != null) {
      threadsMonitoring.startMonitor(ThreadsMonitoring.Mode.PooledExecutor);
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable ex) {
    if (poolStatHelper != null) {
      poolStatHelper.endJob();
    }
    if (threadsMonitoring != null) {
      threadsMonitoring.endMonitor();
    }
  }

  private static int getCorePoolSize(int maxSize) {
    if (maxSize == Integer.MAX_VALUE) {
      return 0;
    }
    return 1;
  }

  /**
   * This handler does a put which will just wait until the queue has room.
   */
  public static class BlockHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException("executor has been shutdown");
      }
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("interrupted", ie);
      }
    }
  }
  /**
   * This handler fronts a synchronous queue, that is owned by the parent ThreadPoolExecutor, with a
   * the
   * client supplied BlockingQueue that supports storage (the buffer queue). A dedicated thread is
   * used to consume off the buffer queue and put into the synchronous queue.
   */
  public static class BufferHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException("executor has been shutdown");
      }
      try {
        PooledExecutorWithDMStats pool = (PooledExecutorWithDMStats) executor;
        pool.blockingWorkQueue.put(r);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("interrupted", ie);
      }
    }
  }

  private static SynchronousQueue<Runnable> getSynchronousQueue(BlockingQueue<Runnable> q) {
    if (q instanceof SynchronousQueue) {
      return (SynchronousQueue<Runnable>) q;
    }
    return new SynchronousQueue<>();
  }

  private static RejectedExecutionHandler newRejectedExecutionHandler(BlockingQueue<Runnable> q) {
    if (q instanceof SynchronousQueue) {
      return new CallerRunsPolicy();
    }
    // create a thread that takes from bufferQueue and puts into result
    return new BufferHandler();
  }
}
