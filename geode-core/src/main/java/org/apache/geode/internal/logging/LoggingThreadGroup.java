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
package org.apache.geode.internal.logging;

import static org.apache.geode.logging.spi.LogWriterLevel.ALL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.Assert;

/**
 * A <code>ThreadGroup</code> that logs all {@linkplain #uncaughtException uncaught exceptions} to a
 * GemFire <code>LogWriterI18n</code>. It also keeps track of the uncaught exceptions that were
 * thrown by its threads. This is comes in handy when a thread fails to initialize properly (see bug
 * 32550).
 *
 * @see LoggingThreadGroup#createThreadGroup
 *
 * @since GemFire 4.0
 */
public class LoggingThreadGroup extends ThreadGroup {

  /** A "local" log writer that logs exceptions to standard error */
  @Immutable
  private static final StandardErrorPrinter stderr = new StandardErrorPrinter(ALL.intLevel());

  /** A set of all created LoggingThreadGroups */
  @MakeNotStatic
  private static final Collection<LoggingThreadGroup> loggingThreadGroups = new ArrayList<>();

  /**
   * Returns a <code>ThreadGroup</code> whose {@link ThreadGroup#uncaughtException} method logs to
   * both {#link System#err} and the given <code>InternalLogWriter</code>.
   *
   * @param name The name of the <code>ThreadGroup</code>
   */
  public static LoggingThreadGroup createThreadGroup(final String name) {
    return createThreadGroup(name, (Logger) null);
  }

  /**
   * Returns a <code>ThreadGroup</code> whose {@link ThreadGroup#uncaughtException} method logs to
   * both {#link System#err} and the given <code>InternalLogWriter</code>.
   *
   * @param name The name of the <code>ThreadGroup</code>
   * @param logWriter A <code>InternalLogWriter</code> to log uncaught exceptions to. It is okay for
   *        this argument to be <code>null</code>.
   *
   * @since GemFire 3.0
   */
  public static LoggingThreadGroup createThreadGroup(final String name,
      final InternalLogWriter logWriter) {
    // Cache the LoggingThreadGroups so that we don't create a
    // gazillion of them.
    LoggingThreadGroup group = null;
    synchronized (loggingThreadGroups) {
      for (Iterator<LoggingThreadGroup> iter = loggingThreadGroups.iterator(); iter.hasNext();) {

        LoggingThreadGroup group2 = iter.next();
        if (group2.isDestroyed()) {
          // Clean is this iterator out
          iter.remove();
          continue;
        }

        if (name.equals(group2.getName())) {
          // We already have one!
          // Change the underlying logger to point to new one (creating new
          // thread groups for different loggers leaks groups for repeated
          // connect/disconnects as in dunits for example)
          if (logWriter != group2.logWriter) {
            group2.logWriter = logWriter;
          }
          group = group2;
          break;
        }
      }

      if (group == null) {
        group = new LoggingThreadGroup(name, logWriter);
        // force autoclean to false and not inherit from parent group
        group.setDaemon(false);
        loggingThreadGroups.add(group);
      }
    }

    Assert.assertTrue(!group.isDestroyed());
    return group;
  }

  /**
   * Returns a <code>ThreadGroup</code> whose {@link ThreadGroup#uncaughtException} method logs to
   * both {#link System#err} and the given <code>InternalLogWriter</code>.
   *
   * @param name The name of the <code>ThreadGroup</code>
   * @param logger A <code>InternalLogWriter</code> to log uncaught exceptions to. It is okay for
   *        this argument to be <code>null</code>.
   *
   * @since GemFire 3.0
   */
  public static LoggingThreadGroup createThreadGroup(final String name, final Logger logger) {
    // Cache the LoggingThreadGroups so that we don't create a
    // gazillion of them.
    LoggingThreadGroup group = null;
    synchronized (loggingThreadGroups) {
      for (Iterator<LoggingThreadGroup> iter = loggingThreadGroups.iterator(); iter.hasNext();) {

        LoggingThreadGroup group2 = iter.next();
        if (group2.isDestroyed()) {
          // Clean is this iterator out
          iter.remove();
          continue;
        }

        if (name.equals(group2.getName())) {
          // We already have one!
          // Change the underlying logger to point to new one (creating new
          // thread groups for different loggers leaks groups for repeated
          // connect/disconnects as in dunits for example)
          if (logger != group2.logger) {
            group2.logger = logger;
          }
          group = group2;
          break;
        }
      }

      if (group == null) {
        group = new LoggingThreadGroup(name, logger);
        // force autoclean to false and not inherit from parent group
        group.setDaemon(false);
        loggingThreadGroups.add(group);
      }
    }

    Assert.assertTrue(!group.isDestroyed());
    return group;
  }

  public static void cleanUpThreadGroups() {
    synchronized (loggingThreadGroups) {
      for (LoggingThreadGroup loggingThreadGroup : loggingThreadGroups) {
        if (!loggingThreadGroup.getName().equals(InternalDistributedSystem.SHUTDOWN_HOOK_NAME)
            && !loggingThreadGroup.getName().equals("GemFireConnectionFactory Shutdown Hook")) {
          loggingThreadGroup.cleanup();
        }
      }
    }
  }

  /**
   * Note: Must be used for test purposes ONLY.
   *
   * @return thread group with given name.
   */
  public static ThreadGroup getThreadGroup(final String threadGroupName) {
    synchronized (loggingThreadGroups) {
      for (Object object : loggingThreadGroups) {
        LoggingThreadGroup threadGroup = (LoggingThreadGroup) object;
        if (threadGroup.getName().equals(threadGroupName)) {
          return threadGroup;
        }
      }
      return null;
    }
  }

  /**
   * A log writer that the user has specified for logging uncaught exceptions.
   */
  protected volatile InternalLogWriter logWriter;

  /**
   * A logger that the user has specified for logging uncaught exceptions.
   */
  protected volatile Logger logger;

  /**
   * The count uncaught exceptions that were thrown by threads in this thread group.
   */
  private long uncaughtExceptionsCount;

  /**
   * Creates a new <code>LoggingThreadGroup</code> that logs uncaught exceptions to the given log
   * writer.
   *
   * @param name The name of the thread group
   * @param logWriter A logWriter to which uncaught exceptions are logged. May be <code>null</code>.
   */
  LoggingThreadGroup(final String name, final InternalLogWriter logWriter) {
    super(name);
    this.logWriter = logWriter;
  }

  /**
   * Creates a new <code>LoggingThreadGroup</code> that logs uncaught exceptions to the given
   * logger.
   *
   * @param name The name of the thread group
   * @param logger A logger to which uncaught exceptions are logged. May be <code>null</code>.
   */
  LoggingThreadGroup(final String name, final Logger logger) {
    super(name);
    this.logger = logger;
  }

  private final Object dispatchLock = new Object();

  /**
   * Logs an uncaught exception to a log writer
   */
  @Override
  public void uncaughtException(final Thread t, final Throwable ex) {
    synchronized (dispatchLock) {
      if (ex instanceof VirtualMachineError) {
        SystemFailure.setFailure((VirtualMachineError) ex); // don't throw
      }
      // Solution to treat the shutdown hook error as a special case.
      // Do not change the hook's thread name without also changing it here.
      String threadName = t.getName();
      if ((ex instanceof NoClassDefFoundError)
          && (threadName.equals(InternalDistributedSystem.SHUTDOWN_HOOK_NAME))) {
        final String msg =
            "Uncaught exception in thread %s this message can be disregarded if it occurred during an Application Server shutdown. The Exception message was: %s";
        final Object[] msgArgs = new Object[] {t, ex.getLocalizedMessage()};
        stderr.info(String.format(msg, msgArgs));
        if (logger != null) {
          logger.info(String.format(msg, msgArgs));
        }
        if (logWriter != null) {
          logWriter.info(String.format(msg, msgArgs));
        }
      } else {
        stderr.severe(String.format("Uncaught exception in thread %s", t), ex);
        if (logger != null) {
          logger.fatal(String.format("Uncaught exception in thread %s", t), ex);
        }
        if (logWriter != null) {
          logWriter.severe(String.format("Uncaught exception in thread %s", t), ex);
        }
      }
      uncaughtExceptionsCount++;
    }
  }


  /**
   * clear number of uncaught exceptions
   */
  public void clearUncaughtExceptionsCount() {
    synchronized (dispatchLock) {
      uncaughtExceptionsCount = 0;
    }
  }

  /**
   * Returns the number of uncaught exceptions that occurred in threads in this thread group.
   */
  public long getUncaughtExceptionsCount() {
    synchronized (dispatchLock) {
      return uncaughtExceptionsCount;
    }
  }

  /**
   * clean up the threadgroup, releasing resources that could be problematic (bug 35388)
   *
   * @since GemFire 4.2.3
   */
  public synchronized void cleanup() {
    // the logwriter holds onto a distribution config, which holds onto
    // the InternalDistributedSystem, which holds onto the
    // DistributionManager, which holds onto ... you get the idea
    logger = null;
    logWriter = null;
  }
}
