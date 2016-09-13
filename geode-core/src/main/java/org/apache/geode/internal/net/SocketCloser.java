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
package com.gemstone.gemfire.internal.net;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;

/**
 * This class allows sockets to be closed without blocking.
 * In some cases we have seen a call of socket.close block for minutes.
 * This class maintains a thread pool for every other member we have
 * connected sockets to. Any request to close by default returns immediately
 * to the caller while the close is called by a background thread.
 * The requester can wait for a configured amount of time by setting
 * the "p2p.ASYNC_CLOSE_WAIT_MILLISECONDS" system property.
 * Idle threads that are not doing a close will timeout after 2 minutes.
 * This can be configured by setting the
 * "p2p.ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS" system property.
 * A pool exists for each remote address that we have a socket connected to.
 * That way if close is taking a long time to one address we can still get closes
 * done to another address.
 * Each address pool by default has at most 8 threads. This max threads can be
 * configured using the "p2p.ASYNC_CLOSE_POOL_MAX_THREADS" system property.
 */
public class SocketCloser {
  private static final Logger logger = LogService.getLogger();
  /** Number of seconds to wait before timing out an unused async close thread. Default is 120 (2 minutes). */
  static final long ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS = Long.getLong("p2p.ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS", 120).longValue();
  /** Maximum number of threads that can be doing a socket close. Any close requests over this max will queue up waiting for a thread. */
  static final int ASYNC_CLOSE_POOL_MAX_THREADS = Integer.getInteger("p2p.ASYNC_CLOSE_POOL_MAX_THREADS", 8).intValue();
  /** How many milliseconds the synchronous requester waits for the async close to happen. Default is 0. Prior releases waited 50ms. */ 
  static final long ASYNC_CLOSE_WAIT_MILLISECONDS = Long.getLong("p2p.ASYNC_CLOSE_WAIT_MILLISECONDS", 0).longValue();
  

  /** map of thread pools of async close threads */
  private final HashMap<String, ThreadPoolExecutor> asyncCloseExecutors = new HashMap<>();
  private final long asyncClosePoolKeepAliveSeconds;
  private final int asyncClosePoolMaxThreads;
  private final long asyncCloseWaitTime;
  private final TimeUnit asyncCloseWaitUnits;
  private boolean closed;
  
  public SocketCloser() {
    this(ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS, ASYNC_CLOSE_POOL_MAX_THREADS, ASYNC_CLOSE_WAIT_MILLISECONDS, TimeUnit.MILLISECONDS);
  }
  public SocketCloser(int asyncClosePoolMaxThreads, long asyncCloseWaitMillis) {
    this(ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS, asyncClosePoolMaxThreads, asyncCloseWaitMillis, TimeUnit.MILLISECONDS);
  }
  public SocketCloser(long asyncClosePoolKeepAliveSeconds, int asyncClosePoolMaxThreads, long asyncCloseWaitTime, TimeUnit asyncCloseWaitUnits) {
    this.asyncClosePoolKeepAliveSeconds = asyncClosePoolKeepAliveSeconds;
    this.asyncClosePoolMaxThreads = asyncClosePoolMaxThreads;
    this.asyncCloseWaitTime = asyncCloseWaitTime;
    this.asyncCloseWaitUnits = asyncCloseWaitUnits;
  }
  
  public int getMaxThreads() {
    return this.asyncClosePoolMaxThreads;
  }

  private ThreadPoolExecutor getAsyncThreadExecutor(String address) {
    synchronized (asyncCloseExecutors) {
      ThreadPoolExecutor pool = asyncCloseExecutors.get(address);
      if (pool == null) {
        final ThreadGroup tg = LoggingThreadGroup.createThreadGroup("Socket asyncClose", logger);
        ThreadFactory tf = new ThreadFactory() { 
          public Thread newThread(final Runnable command) { 
            Thread thread = new Thread(tg, command); 
            thread.setDaemon(true);
            return thread;
          } 
        }; 
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(); 
        pool = new ThreadPoolExecutor(this.asyncClosePoolMaxThreads, this.asyncClosePoolMaxThreads, this.asyncClosePoolKeepAliveSeconds, TimeUnit.SECONDS, workQueue, tf);
        pool.allowCoreThreadTimeOut(true);
        asyncCloseExecutors.put(address, pool);
      }
      return pool;
    }
  }
  /**
   * Call this method if you know all the resources in the closer
   * for the given address are no longer needed.
   * Currently a thread pool is kept for each address and if you
   * know that an address no longer needs its pool then you should
   * call this method.
   */
  public void releaseResourcesForAddress(String address) {
    synchronized (asyncCloseExecutors) {
      ThreadPoolExecutor pool = asyncCloseExecutors.get(address);
      if (pool != null) {
        pool.shutdown();
        asyncCloseExecutors.remove(address);
      }
    }
  }
  private boolean isClosed() {
    synchronized (asyncCloseExecutors) {
      return this.closed;
    }
  }
  /**
   * Call close when you are all done with your socket closer.
   * If you call asyncClose after close is called then the
   * asyncClose will be done synchronously.
   */
  public void close() {
    synchronized (asyncCloseExecutors) {
      if (!this.closed) {
        this.closed = true;
        for (ThreadPoolExecutor pool: asyncCloseExecutors.values()) {
          pool.shutdown();
        }
        asyncCloseExecutors.clear();
      }
    }
  }
  private void asyncExecute(String address, Runnable r) {
    // Waiting 50ms for the async close request to complete is what the old (close per thread)
    // code did. But now that we will not create a thread for every close request
    // it seems better to let the thread that requested the close to move on quickly.
    // So the default has changed to not wait. The system property p2p.ASYNC_CLOSE_WAIT_MILLISECONDS
    // can be set to how many milliseconds to wait.
    if (this.asyncCloseWaitTime == 0) {
      getAsyncThreadExecutor(address).execute(r);
    } else {
      Future<?> future = getAsyncThreadExecutor(address).submit(r);
      try {
        future.get(this.asyncCloseWaitTime, this.asyncCloseWaitUnits);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        // We want this code to wait at most 50ms for the close to happen.
        // It is ok to ignore these exception and let the close continue
        // in the background.
      }
    }
  }
  /**
   * Closes the specified socket in a background thread.
   * In some cases we see close hang (see bug 33665).
   * Depending on how the SocketCloser is configured (see ASYNC_CLOSE_WAIT_MILLISECONDS)
   * this method may block for a certain amount of time.
   * If it is called after the SocketCloser is closed then a normal
   * synchronous close is done.
   * @param sock the socket to close
   * @param address identifies who the socket is connected to
   * @param extra an optional Runnable with stuff to execute in the async thread
   */
  public void asyncClose(final Socket sock, final String address, final Runnable extra) {
    if (sock == null || sock.isClosed()) {
      return;
    }
    boolean doItInline = false;
    try {
      synchronized (asyncCloseExecutors) {
        if (isClosed()) {
          // this SocketCloser has been closed so do a synchronous, inline, close
          doItInline = true;
        } else {
          asyncExecute(address, new Runnable() {
            public void run() {
              Thread.currentThread().setName("AsyncSocketCloser for " + address);
              try {
                if (extra != null) {
                  extra.run();
                }
                inlineClose(sock);
              } finally {
                Thread.currentThread().setName("unused AsyncSocketCloser");
              }
            }
          });
        }
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
      inlineClose(sock);
    }
  }
  

  /**
   * Closes the specified socket
   * @param sock the socket to close
   */
  private static void inlineClose(final Socket sock) {
    // the next two statements are a mad attempt to fix bug
    // 36041 - segv in jrockit in pthread signaling code.  This
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
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.security.ProviderException pe) {
      // some ssl implementations have trouble with termination and throw
      // this exception.  See bug #40783
    } catch (Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // Sun's NIO implementation has been known to throw Errors
      // that are caused by IOExceptions.  If this is the case, it's
      // okay.
      if (e.getCause() instanceof IOException) {
        // okay...
      } else {
        throw e;
      }
    }
  }
}
