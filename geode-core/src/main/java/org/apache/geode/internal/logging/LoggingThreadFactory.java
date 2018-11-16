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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Threads produced by instances of this class will always
 * log uncaught exceptions. They will also always be daemon
 * and have unique names that contain the "baseName" passed
 * to the constructor.
 * <p>
 * What happens each time a thread is created can be customized
 * using the optional "threadInitializer".
 * <p>
 * What happens each time a thread is run can be customized
 * using the optional "commandWrapper".
 */
public class LoggingThreadFactory implements ThreadFactory {

  private final String baseName;
  private final CommandWrapper commandWrapper;
  private final ThreadInitializer threadInitializer;
  private final boolean isDaemon;
  private final AtomicInteger threadCount = new AtomicInteger(1);

  public interface ThreadInitializer {
    public void initialize(Thread thread);
  }

  public interface CommandWrapper {
    /**
     * Invoke the current method passing it a runnable that it can
     * decide to call when it wants.
     */
    public void invoke(Runnable runnable);
  }

  /**
   * Create a factory that produces daemon threads that log uncaught exceptions
   *
   * @param baseName the base name will be included in every thread name
   * @param threadInitializer if not null, will be invoked with the thread each time a thread is
   *        created
   * @param commandWrapper if not null, will be invoked by each thread created by this factory
   * @param isDaemon true if threads will be daemons
   */
  public LoggingThreadFactory(String baseName, ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper, boolean isDaemon) {
    this.baseName = baseName;
    this.threadInitializer = threadInitializer;
    this.commandWrapper = commandWrapper;
    this.isDaemon = isDaemon;
  }

  /**
   * Create a factory that produces daemon threads that log uncaught exceptions
   *
   * @param baseName the base name will be included in every thread name
   * @param threadInitializer if not null, will be invoked with the thread each time a thread is
   *        created
   * @param commandWrapper if not null, will be invoked by each thread created by this factory
   */
  public LoggingThreadFactory(String baseName, ThreadInitializer threadInitializer,
      CommandWrapper commandWrapper) {
    this(baseName, threadInitializer, commandWrapper, true);
  }

  /**
   * Create a factory that produces daemon threads that log uncaught exceptions
   *
   * @param baseName the base name will be included in every thread name
   */
  public LoggingThreadFactory(String baseName) {
    this(baseName, null, null, true);
  }

  /**
   * Create a factory that produces threads that log uncaught exceptions
   *
   * @param baseName the base name will be included in every thread name
   * @param isDaemon true if threads will be daemons
   */
  public LoggingThreadFactory(String baseName, boolean isDaemon) {
    this(baseName, null, null, isDaemon);
  }

  /**
   * Create a factory that produces daemon threads that log uncaught exceptions
   *
   * @param baseName the base name will be included in every thread name
   * @param commandWrapper if not null, will be invoked by each thread created by this factory
   */
  public LoggingThreadFactory(String baseName, CommandWrapper commandWrapper) {
    this(baseName, null, commandWrapper, true);
  }

  private String getUniqueName() {
    return this.baseName + threadCount.getAndIncrement();
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Runnable commandToRun;
    if (this.commandWrapper != null) {
      commandToRun = () -> this.commandWrapper.invoke(runnable);
    } else {
      commandToRun = runnable;
    }
    Thread thread = new LoggingThread(getUniqueName(), isDaemon, commandToRun);
    if (this.threadInitializer != null) {
      this.threadInitializer.initialize(thread);
    }
    return thread;
  }
}
