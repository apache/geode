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

import static org.apache.geode.distributed.internal.OperationExecutors.FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

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

  private static final Logger logger = LogService.getLogger();

  private static final int OFFER_TIME =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "RETRY_INTERVAL", 5000).intValue();

  /**
   * Identifier for function execution threads and any of their children
   */
  @MakeNotStatic()
  private static final InheritableThreadLocal<Boolean> isFunctionExecutionThread =
      new InheritableThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return Boolean.FALSE;
        }
      };


  /**
   * Is the current thread used for executing Functions?
   */
  public static boolean isFunctionExecutionThread() {
    return isFunctionExecutionThread.get().booleanValue();
  }



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
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
          if (executor.isShutdown()) {
            throw new RejectedExecutionException(
                "executor has been shutdown");
          } else {
            if (isBufferConsumer(executor)) {
              handleRejectedExecutionForBufferConsumer(r, executor);
            } else if (isFunctionExecutionThread()) {
              handleRejectedExecutionForFunctionExecutionThread(r, executor);
            } else {
              // In the normal case, add the rejected request to the blocking queue.
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

        private boolean isBufferConsumer(ThreadPoolExecutor executor) {
          return Thread
              .currentThread() == ((FunctionExecutionPooledExecutor) executor).bufferConsumer;
        }

        private boolean isFunctionExecutionThread() {
          return isFunctionExecutionThread.get();
        }

        /**
         * Handle rejected execution for the bufferConsumer (the thread that takes from the blocking
         * queue and offers to the synchronous queue). Spin off a thread directly in this case,
         * since an offer has already been made to the synchronous queue and failed. This means that
         * all the function execution threads are in use.
         */
        private void handleRejectedExecutionForBufferConsumer(Runnable r,
            ThreadPoolExecutor executor) {
          logger.warn("An additional " + FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX
              + " thread is being launched because all " + executor.getMaximumPoolSize()
              + " thread pool threads are in use for greater than " + OFFER_TIME + " ms");
          launchAdditionalThread(r, executor);
        }

        /**
         * Handle rejected execution for a function execution thread. Spin off a thread directly in
         * this case, since that means a function is executing another function. The child function
         * request shouldn't be in the queue behind the parent request since the parent function is
         * dependent on the child function executing.
         */
        private void handleRejectedExecutionForFunctionExecutionThread(Runnable r,
            ThreadPoolExecutor executor) {
          if (logger.isDebugEnabled()) {
            logger.warn("An additional " + FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX
                + " thread is being launched to prevent slow performance due to nested function executions");
          }
          launchAdditionalThread(r, executor);
        }

        private void launchAdditionalThread(Runnable r, ThreadPoolExecutor executor) {
          Thread th = executor.getThreadFactory().newThread(r);
          th.start();
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
    if (!(q instanceof SynchronousQueue)) {
      this.bufferQueue = q;
      // create a thread that takes from bufferQueue and puts into result
      final BlockingQueue<Runnable> takeQueue = q;
      final BlockingQueue<Runnable> putQueue = getQueue();
      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            for (;;) {
              SystemFailure.checkFailure();
              Runnable task = takeQueue.take();
              if (forFnExec) {
                // In the function case, offer the request to the work queue.
                // If it fails, execute it anyway. This will cause the RejectedExecutionHandler to
                // spin off a thread for it.
                if (!putQueue.offer(task, OFFER_TIME, TimeUnit.MILLISECONDS)) {
                  execute(task);
                }
              } else {
                // In the non-function case, put the request on the work queue.
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

  public static void setIsFunctionExecutionThread(Boolean isExecutionThread) {
    isFunctionExecutionThread.set(isExecutionThread);
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
    /*
     * How long an idle thread will wait, in milliseconds, before it is removed from its thread
     * pool. Default is (30000 * 60) ms (30 minutes). It is not static so it can be set at runtime
     * and pick up different values.
     */
    this(q, poolSize, stats, tf,
        Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "IDLE_THREAD_TIMEOUT", 30000 * 60),
        false /* not for fn exec */, tMonitoring);
  }

  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize,
      PoolStatHelper stats, ThreadFactory tf, boolean forFnExec, ThreadsMonitoring tMonitoring) {
    /*
     * How long an idle thread will wait, in milliseconds, before it is removed from its thread
     * pool. Default is (30000 * 60) ms (30 minutes). It is not static so it can be set at runtime
     * and pick up different values.
     */
    this(q, poolSize, stats, tf,
        Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "IDLE_THREAD_TIMEOUT", 30000 * 60),
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
    @Override
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
    @Override
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
