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
  private static final Logger logger = LogService.getLogger();
  private static final AtomicInteger uncaughtExceptionsCount = new AtomicInteger();
  private static final UncaughtExceptionHandler handler = (t, ex) -> {
    if (ex instanceof VirtualMachineError) {
      SystemFailure.setFailure((VirtualMachineError) ex); // don't throw
    }
    // Solution to treat the shutdown hook error as a special case.
    // Do not change the hook's thread name without also changing it here.
    if ((ex instanceof NoClassDefFoundError)
        && (t.getName().equals(InternalDistributedSystem.SHUTDOWN_HOOK_NAME))) {
      logger.info(
          "Uncaught exception in thread {0} this message can be disregarded if it occurred during an Application Server shutdown. The Exception message was: {1}",
          t, ex);
    } else {
      String message = MessageFormat.format("Uncaught exception in thread {0}", t);
      logger.fatal(message, ex);
    }
    uncaughtExceptionsCount.incrementAndGet();
  };

  /**
   * Sets the logging uncaught exception handler on the given thread.
   */
  public static void setOnThread(Thread thread) {
    thread.setUncaughtExceptionHandler(handler);
  }

  public static int getUncaughtExceptionsCount() {
    return uncaughtExceptionsCount.get();
  }

  public static void clearUncaughtExceptionsCount() {
    uncaughtExceptionsCount.set(0);
  }

  private LoggingUncaughtExceptionHandler() {
    // no instances allowed
  }
}
