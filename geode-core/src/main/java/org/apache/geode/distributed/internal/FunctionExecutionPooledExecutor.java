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
 * A ThreadPoolExecutor with stat support. This executor also has a buffer that rejected executions
 * spill over into.
 *
 * This executor has also been modified to handle rejected execution in one of three ways: If the
 * executor is for function execution we ignore size caps on the thread pool<br>
 * If the executor has a SynchronousQueue or SynchronousQueueNoSpin then rejected executions are run
 * in the current thread.<br>
 * Otherwise a thread is started in the pool that buffers the rejected tasks and puts them back into
 * the executor.
 *
 */
public class FunctionExecutionPooledExecutor extends ThreadPoolExecutor {
  protected final PoolStatHelper stats;
  private final ThreadsMonitoring threadMonitoring;

  /**
   * Create a new pool
   **/
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int maxPoolSize,
      PoolStatHelper stats, ThreadFactory tf, int msTimeout, RejectedExecutionHandler reh,
      ThreadsMonitoring tMonitoring) {
    super(getCorePoolSize(maxPoolSize), maxPoolSize, msTimeout, TimeUnit.MILLISECONDS, q, tf, reh);
    // if (getCorePoolSize() != 0 && getCorePoolSize() == getMaximumPoolSize()) {
    // allowCoreThreadTimeOut(true); // deadcoded for 1.5
    // }
    this.stats = stats;
    this.threadMonitoring = tMonitoring;
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
  Thread bufferConsumer;

  private static BlockingQueue<Runnable> initQ(BlockingQueue<Runnable> q) {
    if (q instanceof SynchronousQueue) {
      return q;
    } else {
      return new SynchronousQueue<Runnable>();
    }
  }


  private static RejectedExecutionHandler initREH(final BlockingQueue<Runnable> q,
      boolean forFnExec) {
    if (forFnExec) {
      return new RejectedExecutionHandler() {
        public void rejectedExecution(final Runnable r, ThreadPoolExecutor executor) {
          if (executor.isShutdown()) {
            throw new RejectedExecutionException(
                "executor has been shutdown");
          } else {
            // System.out.println("Asif: Rejection called");
            if (Thread
                .currentThread() == ((FunctionExecutionPooledExecutor) executor).bufferConsumer) {
              Thread th = executor.getThreadFactory().newThread((new Runnable() {
                public void run() {
                  r.run();
                }
              }));
              th.start();
            } else {
              try {
                q.put(r);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // this thread is being shutdown so just return;
                return;
              }
            }
          }
        }
      };
    } else {

      if (q instanceof SynchronousQueue) {
        return new CallerRunsPolicy();
        // return new BlockHandler();
      } else {
        // create a thread that takes from bufferQueue and puts into result
        return new BufferHandler();
      }
    }

  }


  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int maxPoolSize,
      PoolStatHelper stats, ThreadFactory tf, int msTimeout, final boolean forFnExec,
      ThreadsMonitoring tMonitoring) {
    this(initQ(q), maxPoolSize, stats, tf, msTimeout, initREH(q, forFnExec), tMonitoring);
    final int retryFor =
        Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "RETRY_INTERVAL", 5000).intValue();
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
              Runnable task = takeQueue.take();
              if (forFnExec) {
                if (!putQueue.offer(task, retryFor, TimeUnit.MILLISECONDS)) {
                  execute(task);
                }
              } else {
                putQueue.put(task);
              }
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
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize,
      PoolStatHelper stats, ThreadFactory tf, ThreadsMonitoring tMonitoring) {
    /**
     * How long an idle thread will wait, in milliseconds, before it is removed from its thread
     * pool. Default is (30000 * 60) ms (30 minutes). It is not static so it can be set at runtime
     * and pick up different values.
     */
    this(q, poolSize, stats, tf,
        Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "IDLE_THREAD_TIMEOUT", 30000 * 60),
        false /* not for fn exec */, tMonitoring);
  }

  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize,
      PoolStatHelper stats, ThreadFactory tf, boolean forFnExec, ThreadsMonitoring tMonitoring) {
    /**
     * How long an idle thread will wait, in milliseconds, before it is removed from its thread
     * pool. Default is (30000 * 60) ms (30 minutes). It is not static so it can be set at runtime
     * and pick up different values.
     */
    this(q, poolSize, stats, tf,
        Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "IDLE_THREAD_TIMEOUT", 30000 * 60),
        forFnExec, tMonitoring);
  }

  /**
   * Default timeout with no stats.
   */
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize, ThreadFactory tf,
      ThreadsMonitoring tMonitoring) {
    this(q, poolSize, null/* no stats */, tf, tMonitoring);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (this.stats != null) {
      this.stats.startJob();
    }
    if (this.threadMonitoring != null) {
      threadMonitoring.startMonitor(ThreadsMonitoring.Mode.FunctionExecutor);
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable ex) {
    if (this.stats != null) {
      this.stats.endJob();
    }
    if (this.threadMonitoring != null) {
      threadMonitoring.endMonitor();
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
   * This handler does a put which will just wait until the queue has room.
   */
  public static class BlockHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException(
            "executor has been shutdown");
      } else {
        try {
          executor.getQueue().put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          RejectedExecutionException e = new RejectedExecutionException(
              "interrupted");
          e.initCause(ie);
          throw e;
        }
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
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException(
            "executor has been shutdown");
      } else {
        try {
          FunctionExecutionPooledExecutor pool = (FunctionExecutionPooledExecutor) executor;
          pool.bufferQueue.put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          RejectedExecutionException e = new RejectedExecutionException(
              "interrupted");
          e.initCause(ie);
          throw e;
        }
      }
    }
  }
}
