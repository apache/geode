/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import java.lang.Thread.UncaughtExceptionHandler;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * This class delegates to a static singleton that handles all
 * exceptions not caught by any thread created in geode.
 * So all interactions with it are done with static methods.
 */
public class LoggingUncaughtExceptionHandler {
  private static final Implementation handler =
      new Implementation(LogService.getLogger(), error -> SystemFailure.setFailure(error));

  public static UncaughtExceptionHandler getInstance() {
    return handler;
  }

  /**
   * Sets the logging uncaught exception handler on the given thread.
   */
  public static void setOnThread(Thread thread) {
    handler.setOnThread(thread);
  }

  public static int getUncaughtExceptionsCount() {
    return handler.getUncaughtExceptionsCount();
  }

  public static void clearUncaughtExceptionsCount() {
    handler.clearUncaughtExceptionsCount();
  }

  LoggingUncaughtExceptionHandler() {
    // no instances allowed
  }

  // non-private for unit testing
  interface FailureSettor {
    void setFailure(VirtualMachineError error);
  }

  // non-private for unit testing
  static class Implementation implements UncaughtExceptionHandler {
    private final Logger logger;
    private final FailureSettor failureSettor;
    private final AtomicInteger uncaughtExceptionsCount = new AtomicInteger();

    Implementation(Logger logger, FailureSettor failureSettor) {
      this.logger = logger;
      this.failureSettor = failureSettor;
    }

    @Override
    public void uncaughtException(Thread t, Throwable ex) {
      if (ex instanceof VirtualMachineError) {
        this.failureSettor.setFailure((VirtualMachineError) ex);
      }
      // Solution to treat the shutdown hook error as a special case.
      // Do not change the hook's thread name without also changing it here.
      if ((ex instanceof NoClassDefFoundError)
          && (t.getName().equals(InternalDistributedSystem.SHUTDOWN_HOOK_NAME))) {
        logger.info(
            "Uncaught exception in thread {} this message can be disregarded if it occurred during an Application Server shutdown. The Exception message was: {}",
            t, ex);
      } else {
        String message = MessageFormat.format("Uncaught exception in thread {0}", t);
        logger.fatal(message, ex);
      }
      uncaughtExceptionsCount.incrementAndGet();
    }

    void setOnThread(Thread thread) {
      thread.setUncaughtExceptionHandler(this);
    }

    int getUncaughtExceptionsCount() {
      return uncaughtExceptionsCount.get();
    }

    void clearUncaughtExceptionsCount() {
      uncaughtExceptionsCount.set(0);
    }
  }
}
