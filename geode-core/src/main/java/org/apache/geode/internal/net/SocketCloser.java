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
package org.apache.geode.internal.net;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LoggingExecutors;

/**
 * This class allows sockets to be closed without blocking. In some cases we have seen a call of
 * socket.close block for minutes. This class maintains a thread pool for every other member we have
 * connected sockets to. Any request to close by default returns immediately to the caller while the
 * close is called by a background thread. The requester can wait for a configured amount of time by
 * setting the "p2p.ASYNC_CLOSE_WAIT_MILLISECONDS" system property. Idle threads that are not doing
 * a close will timeout after 2 minutes. This can be configured by setting the
 * "p2p.ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS" system property. A pool exists for each remote address
 * that we have a socket connected to. That way if close is taking a long time to one address we can
 * still get closes done to another address. Each address pool by default has at most 8 threads.
 * This max threads can be configured using the "p2p.ASYNC_CLOSE_POOL_MAX_THREADS" system property.
 */
public class SocketCloser {

  /**
   * Number of seconds to wait before timing out an unused async close thread. Default is 120 (2
   * minutes).
   */
  static final long ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS =
      Long.getLong("p2p.ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS", 120).longValue();
  /**
   * Maximum number of threads that can be doing a socket close. Any close requests over this max
   * will queue up waiting for a thread.
   */
  static final int ASYNC_CLOSE_POOL_MAX_THREADS =
      Integer.getInteger("p2p.ASYNC_CLOSE_POOL_MAX_THREADS", 4).intValue();
  /**
   * How many milliseconds the synchronous requester waits for the async close to happen. Default is
   * 0. Prior releases waited 50ms.
   */
  static final long ASYNC_CLOSE_WAIT_MILLISECONDS =
      Long.getLong("p2p.ASYNC_CLOSE_WAIT_MILLISECONDS", 0).longValue();

  /**
   * map of thread pools of async close threads
   */
  private final ConcurrentHashMap<String, ExecutorService> asyncCloseExecutors =
      new ConcurrentHashMap<>();
  private final long asyncClosePoolKeepAliveSeconds;
  private final int asyncClosePoolMaxThreads;
  private final long asyncCloseWaitTime;
  private final TimeUnit asyncCloseWaitUnits;
  private Boolean closed = Boolean.FALSE;

  public SocketCloser() {
    this(ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS, ASYNC_CLOSE_POOL_MAX_THREADS,
        ASYNC_CLOSE_WAIT_MILLISECONDS, TimeUnit.MILLISECONDS);
  }

  public SocketCloser(int asyncClosePoolMaxThreads, long asyncCloseWaitMillis) {
    this(ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS, asyncClosePoolMaxThreads, asyncCloseWaitMillis,
        TimeUnit.MILLISECONDS);
  }

  public SocketCloser(long asyncClosePoolKeepAliveSeconds, int asyncClosePoolMaxThreads,
      long asyncCloseWaitTime, TimeUnit asyncCloseWaitUnits) {
    this.asyncClosePoolKeepAliveSeconds = asyncClosePoolKeepAliveSeconds;
    this.asyncClosePoolMaxThreads = asyncClosePoolMaxThreads;
    this.asyncCloseWaitTime = asyncCloseWaitTime;
    this.asyncCloseWaitUnits = asyncCloseWaitUnits;
  }

  public int getMaxThreads() {
    return this.asyncClosePoolMaxThreads;
  }

  private ExecutorService getAsyncThreadExecutor(String address) {
    ExecutorService executorService = asyncCloseExecutors.get(address);
    if (executorService == null) {
      // To be used for pre-1.8 jdk releases.
      // executorService = createThreadPoolExecutor();

      executorService = getWorkStealingPool(asyncClosePoolMaxThreads);

      ExecutorService previousThreadPoolExecutor =
          asyncCloseExecutors.putIfAbsent(address, executorService);

      if (previousThreadPoolExecutor != null) {
        executorService.shutdownNow();
        return previousThreadPoolExecutor;
      }
    }
    return executorService;
  }

  private ExecutorService getWorkStealingPool(int maxParallelThreads) {
    return LoggingExecutors.newWorkStealingPool("SocketCloser-", maxParallelThreads);
  }

  /**
   * Call this method if you know all the resources in the closer for the given address are no
   * longer needed. Currently a thread pool is kept for each address and if you know that an address
   * no longer needs its pool then you should call this method.
   */
  public void releaseResourcesForAddress(String address) {
    ExecutorService executorService = asyncCloseExecutors.remove(address);
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  /**
   * Call close when you are all done with your socket closer. If you call asyncClose after close is
   * called then the asyncClose will be done synchronously.
   */
  public void close() {
    synchronized (closed) {
      if (!this.closed) {
        this.closed = true;
      } else {
        return;
      }
    }
    for (ExecutorService executorService : asyncCloseExecutors.values()) {
      executorService.shutdown();
    }
    asyncCloseExecutors.clear();
  }

  private Future asyncExecute(String address, Runnable runnableToExecute) {
    ExecutorService asyncThreadExecutor = getAsyncThreadExecutor(address);
    return asyncThreadExecutor.submit(runnableToExecute);
  }

  /**
   * Closes the specified socket in a background thread. In some cases we see close hang (see bug
   * 33665). Depending on how the SocketCloser is configured (see ASYNC_CLOSE_WAIT_MILLISECONDS)
   * this method may block for a certain amount of time. If it is called after the SocketCloser is
   * closed then a normal synchronous close is done.
   *
   * @param socket the socket to close
   * @param address identifies who the socket is connected to
   * @param extra an optional Runnable with stuff to execute in the async thread
   */
  public void asyncClose(final Socket socket, final String address, final Runnable extra) {
    if (socket == null || socket.isClosed()) {
      return;
    }
    boolean doItInline = false;
    try {
      Future submittedTask = null;
      synchronized (closed) {
        if (closed) {
          // this SocketCloser has been closed so do a synchronous, inline, close
          doItInline = true;
        } else {
          submittedTask = asyncExecute(address, new Runnable() {
            public void run() {
              Thread.currentThread().setName("AsyncSocketCloser for " + address);
              try {
                if (extra != null) {
                  extra.run();
                }
                inlineClose(socket);
              } finally {
                Thread.currentThread().setName("unused AsyncSocketCloser");
              }
            }
          });
        }
      }
      if (submittedTask != null) {
        waitForFutureTaskWithTimeout(submittedTask);
      }
    } catch (OutOfMemoryError ignore) {
      // If we can't start a thread to close the socket just do it inline.
      // See bug 50573.
      doItInline = true;
    }
    if (doItInline) {
      if (extra != null) {
        extra.run();
      }
      inlineClose(socket);
    }
  }

  private void waitForFutureTaskWithTimeout(Future submittedTask) {
    if (this.asyncCloseWaitTime != 0) {
      try {
        submittedTask.get(this.asyncCloseWaitTime, this.asyncCloseWaitUnits);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        // We want this code to wait at most the asyncCloseWaitTime for the close to happen.
        // It is ok to ignore these exception and let the close continue
        // in the background.
      }
    }
  }

  /**
   * Closes the specified socket
   *
   * @param sock the socket to close
   */
  private static void inlineClose(final Socket sock) {
    // the next two statements are a mad attempt to fix bug
    // 36041 - segv in jrockit in pthread signaling code. This
    // seems to alleviate the problem.
    try {
      sock.shutdownInput();
      sock.shutdownOutput();
    } catch (Exception e) {
    }
    try {
      sock.close();
    } catch (IOException ignore) {
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.security.ProviderException pe) {
      // some ssl implementations have trouble with termination and throw
      // this exception. See bug #40783
    } catch (Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // Sun's NIO implementation has been known to throw Errors
      // that are caused by IOExceptions. If this is the case, it's
      // okay.
      if (e.getCause() instanceof IOException) {
        // okay...
      } else {
        throw e;
      }
    }
  }
}
