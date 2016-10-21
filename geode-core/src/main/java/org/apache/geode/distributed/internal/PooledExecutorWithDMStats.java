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

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.util.List;
import java.util.concurrent.*;

/**
 * A ThreadPoolExecutor with stat support.
 *
 */
public class PooledExecutorWithDMStats extends ThreadPoolExecutor {
  protected final PoolStatHelper stats;

  /**
   * Create a new pool
   **/
  public PooledExecutorWithDMStats(SynchronousQueue<Runnable> q, int maxPoolSize,
      PoolStatHelper stats, ThreadFactory tf, int msTimeout, RejectedExecutionHandler reh) {
    super(getCorePoolSize(maxPoolSize), maxPoolSize, msTimeout, TimeUnit.MILLISECONDS, q, tf, reh);
    // if (getCorePoolSize() != 0 && getCorePoolSize() == getMaximumPoolSize()) {
    // allowCoreThreadTimeOut(true); // deadcoded for 1.5
    // }
    this.stats = stats;
  }

  /**
   * Used to buffer up tasks that would be have been rejected. Only used (i.e. non-null) if
   * constructor queue is not a SynchronousQueue.
   */
  protected BlockingQueue<Runnable> bufferQueue;
  /**
   * Used to consume items off the bufferQueue and put them into the pools synchronous queue. Only
   * used (i.e. non-null) if constructor queue is not a SynchronousQueue.
   */
  private Thread bufferConsumer;

  private static SynchronousQueue<Runnable> initQ(BlockingQueue<Runnable> q) {
    if (q instanceof SynchronousQueue) {
      return (SynchronousQueue<Runnable>) q;
    } else {
      return new SynchronousQueue/* NoSpin */<Runnable>();
    }
  }

  private static RejectedExecutionHandler initREH(BlockingQueue<Runnable> q) {
    if (q instanceof SynchronousQueue) {
      return new CallerRunsPolicy();
      // return new BlockHandler();
    } else {
      // create a thread that takes from bufferQueue and puts into result
      return new BufferHandler();
    }
  }

  /**
   * Create a new pool that uses the supplied Channel for queuing, and with all default parameter
   * settings except for pool size.
   **/
  public PooledExecutorWithDMStats(BlockingQueue<Runnable> q, int maxPoolSize, PoolStatHelper stats,
      ThreadFactory tf, int msTimeout) {
    this(initQ(q), maxPoolSize, stats, tf, msTimeout, initREH(q));
    if (!(q instanceof SynchronousQueue)) {
      this.bufferQueue = q;
      // create a thread that takes from bufferQueue and puts into result
      final BlockingQueue<Runnable> takeQueue = q;
      final BlockingQueue<Runnable> putQueue = getQueue();
      Runnable r = new Runnable() {
        public void run() {
          try {
            for (;;) {
              SystemFailure.checkFailure();
              Runnable job = takeQueue.take();
              putQueue.put(job);
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // this thread is being shutdown so just return;
            return;
          }
        }
      };
      this.bufferConsumer = tf.newThread(r);
      this.bufferConsumer.start();
    }
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
    if (this.bufferConsumer != null) {
      this.bufferConsumer.interrupt();
    }
    super.terminated();
  }

  @Override
  public List shutdownNow() {
    terminated();
    List l = super.shutdownNow();
    if (this.bufferQueue != null) {
      this.bufferQueue.drainTo(l);
    }
    return l;
  }

  /**
   * Sets timeout to IDLE_THREAD_TIMEOUT
   */
  public PooledExecutorWithDMStats(BlockingQueue<Runnable> q, int poolSize, PoolStatHelper stats,
      ThreadFactory tf) {
    /**
     * How long an idle thread will wait, in milliseconds, before it is removed from its thread
     * pool. Default is (30000 * 60) ms (30 minutes). It is not static so it can be set at runtime
     * and pick up different values.
     */
    this(q, poolSize, stats, tf,
        Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "IDLE_THREAD_TIMEOUT", 30000 * 60)
            .intValue());
  }

  /**
   * Default timeout with no stats.
   */
  public PooledExecutorWithDMStats(BlockingQueue<Runnable> q, int poolSize, ThreadFactory tf) {
    this(q, poolSize, null/* no stats */, tf);
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

  private static int getCorePoolSize(int maxSize) {
    if (maxSize == Integer.MAX_VALUE) {
      return 0;
    } else {
      return 1;
      // int result = Runtime.getRuntime().availableProcessors();
      // if (result < 2) {
      // result = 2;
      // }
      // if (result > maxSize) {
      // result = maxSize;
      // }
      // return result;
    }
  }

  /**
   * This guy does a put which will just wait until the queue has room.
   */
  public static class BlockHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException(
            LocalizedStrings.PooledExecutorWithDMStats_EXECUTOR_HAS_BEEN_SHUTDOWN
                .toLocalizedString());
      } else {
        try {
          executor.getQueue().put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          RejectedExecutionException e = new RejectedExecutionException(
              LocalizedStrings.PooledExecutorWithDMStats_INTERRUPTED.toLocalizedString());
          e.initCause(ie);
          throw e;
        }
      }
    }
  }
  /**
   * This guy fronts a synchronous queue, that is owned by the parent ThreadPoolExecutor, with a the
   * client supplied BlockingQueue that supports storage (the buffer queue). A dedicated thread is
   * used to consume off the buffer queue and put into the synchronous queue.
   */
  public static class BufferHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException(
            LocalizedStrings.PooledExecutorWithDMStats_EXECUTOR_HAS_BEEN_SHUTDOWN
                .toLocalizedString());
      } else {
        try {
          PooledExecutorWithDMStats pool = (PooledExecutorWithDMStats) executor;
          pool.bufferQueue.put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          RejectedExecutionException e = new RejectedExecutionException(
              LocalizedStrings.PooledExecutorWithDMStats_INTERRUPTED.toLocalizedString());
          e.initCause(ie);
          throw e;
        }
      }
    }
  }
}
