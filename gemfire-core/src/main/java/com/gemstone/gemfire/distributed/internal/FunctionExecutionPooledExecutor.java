/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;

import java.util.List;

/**
 * A ThreadPoolExecutor with stat support.  This executor also has a
 * buffer that rejected executions spill over into.
 * 
 * This executor has also been modified to handle rejected execution in
 * one of three ways:
 *   If the executor is for function execution we ignore size caps on the thread pool<br>
 *   If the executor has a SynchronousQueue or SynchronousQueueNoSpin then
 *   rejected executions are run in the current thread.<br>
 *   Otherwise a thread is started in the pool that buffers the rejected
 *   tasks and puts them back into the executor.
 *   
 *   TODO this is a version of PooledExecutorWithDMStats that was spun off
 *   in cedar_dev_Oct12 as a measure to fix bug #46438.  It should be cleaned
 *   up to not have code paths for !forFnExecution since it is only used
 *   for function execution.  This was not done for the 8.0 release because
 *   of code freeze.
 * 
 * @author darrel
 *
 */
public class FunctionExecutionPooledExecutor extends ThreadPoolExecutor {
  protected final PoolStatHelper stats;
  
  /** 
   * Create a new pool
   **/
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int maxPoolSize, PoolStatHelper stats, ThreadFactory tf, int msTimeout, RejectedExecutionHandler reh) {
    super(getCorePoolSize(maxPoolSize), maxPoolSize,
          msTimeout, TimeUnit.MILLISECONDS,
          q, tf, reh);
//     if (getCorePoolSize() != 0 && getCorePoolSize() == getMaximumPoolSize()) {
//       allowCoreThreadTimeOut(true); // deadcoded for 1.5
//     }
    this.stats = stats;
  }

  /**
   * Used to buffer up tasks that would be have been rejected.
   * Only used (i.e. non-null) if constructor queue is not a SynchronousQueue.
   */
  protected BlockingQueue<Runnable> bufferQueue;
  /**
   * Used to consume items off the bufferQueue and put them into the pools
   * synchronous queue.
   * Only used (i.e. non-null) if constructor queue is not a SynchronousQueue.
   */
  Thread bufferConsumer;
  
  private static BlockingQueue<Runnable> initQ(BlockingQueue<Runnable> q) {
    if (q instanceof SynchronousQueue) {
      return q;
    } else {
      return new SynchronousQueue<Runnable>();
    }
  }

 
  private static RejectedExecutionHandler initREH(
      final BlockingQueue<Runnable> q, boolean forFnExec)
  {
    if (forFnExec) {
      return new RejectedExecutionHandler() {
        public void rejectedExecution(final Runnable r,
            ThreadPoolExecutor executor)
        {
          if (executor.isShutdown()) {
            throw new RejectedExecutionException(
                LocalizedStrings.PooledExecutorWithDMStats_EXECUTOR_HAS_BEEN_SHUTDOWN
                    .toLocalizedString());
          }
          else {
            // System.out.println("Asif: Rejection called");
            if (Thread.currentThread() == ((FunctionExecutionPooledExecutor)executor).bufferConsumer) {
              Thread th = executor.getThreadFactory().newThread(
                  (new Runnable() {
                    public void run()
                    {
                      r.run();
                    }
                  }));
              th.start();
            }
            else {
              try {
                q.put(r);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // this thread is being shutdown so just return;
                return;
              }
            }
          }
        }
      };
    }
    else {

      if (q instanceof SynchronousQueue) {
        return new CallerRunsPolicy();
        // return new BlockHandler();
      }
      else {
        // create a thread that takes from bufferQueue and puts into result
        return new BufferHandler();
      }
    }

  }
  
 
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int maxPoolSize, PoolStatHelper stats, ThreadFactory tf, int msTimeout, final boolean forFnExec) {
    this(initQ(q), maxPoolSize, stats, tf, msTimeout, initREH(q,forFnExec));
    final int retryFor = Integer.getInteger("gemfire.RETRY_INTERVAL", 5000).intValue(); 
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
                if(forFnExec) {
                   if(!putQueue.offer(task,retryFor , TimeUnit.MILLISECONDS)){
                     submit(task);  
                   }                   
                }else {
                  putQueue.put(task);
                }               
              }
            }
            catch (InterruptedException ie) {
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
    }
    finally {
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
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize, PoolStatHelper stats, ThreadFactory tf) {
  /**
   * How long an idle thread will wait, in milliseconds, before it is removed
   * from its thread pool. Default is (30000 * 60) ms (30 minutes).
   * It is not static so it can be set at runtime and pick up different values.
   */
    this(q, poolSize, stats, tf, Integer.getInteger("gemfire.IDLE_THREAD_TIMEOUT", 30000*60), false /* not for fn exec*/);
  }
  
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize, PoolStatHelper stats, ThreadFactory tf, boolean forFnExec) {
    /**
     * How long an idle thread will wait, in milliseconds, before it is removed
     * from its thread pool. Default is (30000 * 60) ms (30 minutes).
     * It is not static so it can be set at runtime and pick up different values.
     */
      this(q, poolSize, stats, tf, Integer.getInteger("gemfire.IDLE_THREAD_TIMEOUT", 30000*60), forFnExec);
    }
  /**
   * Default timeout with no stats.
   */
  public FunctionExecutionPooledExecutor(BlockingQueue<Runnable> q, int poolSize, ThreadFactory tf) {
    this(q, poolSize, null/*no stats*/, tf);
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
//       int result = Runtime.getRuntime().availableProcessors();
//       if (result < 2) {
//         result = 2;
//       }
//       if (result > maxSize) {
//         result = maxSize;
//       }
//       return result;
    }
  }
  
  /**
   * This guy does a put which will just wait until the queue has room.
   */
  public static class BlockHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException(LocalizedStrings.PooledExecutorWithDMStats_EXECUTOR_HAS_BEEN_SHUTDOWN.toLocalizedString());
      } else {
        try {
          executor.getQueue().put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          RejectedExecutionException e = new RejectedExecutionException(LocalizedStrings.PooledExecutorWithDMStats_INTERRUPTED.toLocalizedString());
          e.initCause(ie);
          throw e;
        }
      }
    }
  }
  /**
   * This guy fronts a synchronous queue, that is owned by the parent
   * ThreadPoolExecutor, with a the client supplied BlockingQueue that
   * supports storage (the buffer queue).
   * A dedicated thread is used to consume off the buffer queue and put
   * into the synchronous queue.
   */
  public static class BufferHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException(LocalizedStrings.PooledExecutorWithDMStats_EXECUTOR_HAS_BEEN_SHUTDOWN.toLocalizedString());
      } else {
        try {
          FunctionExecutionPooledExecutor pool = (FunctionExecutionPooledExecutor)executor;
          pool.bufferQueue.put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          RejectedExecutionException e = new RejectedExecutionException(LocalizedStrings.PooledExecutorWithDMStats_INTERRUPTED.toLocalizedString());
          e.initCause(ie);
          throw e;
        }
      }
    }
  }
}
