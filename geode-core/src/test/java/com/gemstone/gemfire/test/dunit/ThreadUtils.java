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
package com.gemstone.gemfire.test.dunit;

import static org.junit.Assert.fail;
import static com.gemstone.gemfire.test.dunit.Jitter.*;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * <code>ThreadUtils</code> provides static utility methods to perform thread
 * related actions such as dumping thread stacks.
 * 
 * These methods can be used directly: <code>ThreadUtils.dumpAllStacks()</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.ThreadUtils.*;
 *    ...
 *    dumpAllStacks();
 * </pre>
 *
 * Extracted from DistributedTestCase.
 */
public class ThreadUtils {
  
  private static final Logger logger = LogService.getLogger();

  protected ThreadUtils() {
  }
  
  /**
   * Print stack dumps for all vms.
   * 
   * @author bruce
   * @since 5.0
   */
  public static void dumpAllStacks() {
    for (int h=0; h < Host.getHostCount(); h++) {
      dumpStack(Host.getHost(h));
    }
  }
  
  /**
   * Dump all thread stacks
   */
  public static void dumpMyThreads() {
    OSProcess.printStacks(0, false);
  }

  /** 
   * Print a stack dump for this vm.
   * 
   * @author bruce
   * @since 5.0
   */
  public static void dumpStack() {
    OSProcess.printStacks(0, false);
  }

  /** 
   * Print stack dumps for all vms on the given host.
   * 
   * @author bruce
   * @since 5.0
   */
  public static void dumpStack(final Host host) {
    for (int v=0; v < host.getVMCount(); v++) {
      host.getVM(v).invoke(com.gemstone.gemfire.test.dunit.DistributedTestCase.class, "dumpStack");
    }
  }

  /** 
   * Print a stack dump for the given vm.
   * 
   * @author bruce
   * @since 5.0
   */
  public static void dumpStack(final VM vm) {
    vm.invoke(com.gemstone.gemfire.test.dunit.DistributedTestCase.class, "dumpStack");
  }

  public static void dumpStackTrace(final Thread thread, final StackTraceElement[] stackTrace) {
    StringBuilder msg = new StringBuilder();
    msg.append("Thread=<")
      .append(thread)
      .append("> stackDump:\n");
    for (int i=0; i < stackTrace.length; i++) {
      msg.append("\t")
        .append(stackTrace[i])
        .append("\n");
    }
    logger.info(msg.toString());
  }

  /**
   * Wait for a thread to join.
   * 
   * @param thread thread to wait on
   * @param timeoutMilliseconds maximum time to wait
   * @throws AssertionError if the thread does not terminate
   */
  public static void join(final Thread thread, final long timeoutMilliseconds) {
    final long tilt = System.currentTimeMillis() + timeoutMilliseconds;
    final long incrementalWait = jitterInterval(timeoutMilliseconds);
    final long start = System.currentTimeMillis();
    for (;;) {
      // I really do *not* understand why this check is necessary
      // but it is, at least with JDK 1.6.  According to the source code
      // and the javadocs, one would think that join() would exit immediately
      // if the thread is dead.  However, I can tell you from experimentation
      // that this is not the case. :-(  djp 2008-12-08
      if (!thread.isAlive()) {
        break;
      }
      try {
        thread.join(incrementalWait);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
      if (System.currentTimeMillis() >= tilt) {
        break;
      }
    } // for
    if (thread.isAlive()) {
      logger.info("HUNG THREAD");
      ThreadUtils.dumpStackTrace(thread, thread.getStackTrace());
      ThreadUtils.dumpMyThreads();
      thread.interrupt(); // We're in trouble!
      fail("Thread did not terminate after " + timeoutMilliseconds + " ms: " + thread);
    }
    long elapsedMs = (System.currentTimeMillis() - start);
    if (elapsedMs > 0) {
      String msg = "Thread " + thread + " took " + elapsedMs + " ms to exit.";
      logger.info(msg);
    }
  }
}
