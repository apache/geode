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
package org.apache.geode.logging.internal.executors;

/**
 * LoggingThread instances always handle uncaught exceptions by logging them.
 */
public class LoggingThread extends Thread {
  private final boolean treatExceptionAsFatal;

  /**
   * Creates a daemon thread with the given name
   * that logs uncaught exceptions.
   *
   * @param name the name of the thread
   */
  public LoggingThread(final String name) {
    this(name, null);
  }

  /**
   * Creates a daemon thread with the given name and runnable
   * that logs uncaught exceptions.
   *
   * @param name the name of the thread
   * @param runnable what the thread will run
   */
  public LoggingThread(final String name, final Runnable runnable) {
    this(name, true, runnable);
  }

  /**
   * Creates a thread with the given name and runnable
   * that logs uncaught exceptions.
   *
   * @param name the name of the thread
   * @param isDaemon true if thread will be marked as a daemon
   * @param runnable what the thread will run
   */
  public LoggingThread(final String name, final boolean isDaemon, final Runnable runnable) {
    this(name, isDaemon, runnable, true);
  }

  public LoggingThread(final String name, final boolean isDaemon, final Runnable runnable,
      final boolean treatExceptionAsFatal) {
    super(runnable, name);
    this.treatExceptionAsFatal = treatExceptionAsFatal;
    setDaemon(isDaemon);
    LoggingUncaughtExceptionHandler.setOnThread(this);
  }

  public boolean isTreatExceptionAsFatal() {
    return treatExceptionAsFatal;
  }
}
