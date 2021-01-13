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

package org.apache.geode.internal.monitoring;

import java.util.Map;

import org.apache.geode.internal.monitoring.executor.AbstractExecutor;

public interface ThreadsMonitoring {

  public enum Mode {
    FunctionExecutor,
    PooledExecutor,
    SerialQueuedExecutor,
    OneTaskOnlyExecutor,
    ScheduledThreadExecutor,
    AGSExecutor,
    P2PReaderExecutor
  };

  Map<Long, AbstractExecutor> getMonitorMap();

  /**
   * Closes this ThreadMonitoring and releases all resources associated with it.
   */
  void close();

  /**
   * Start monitoring the calling thread.
   *
   * @param mode describes the group the calling thread should be associated with.
   * @return true - if succeeded , false - if failed.
   */
  public boolean startMonitor(Mode mode);

  /**
   * Stops monitoring the calling thread if it is currently being monitored.
   */
  public void endMonitor();

  /**
   * Creates a new executor that is associated with the calling thread.
   * Callers need to pass the returned executor to {@link #register(AbstractExecutor)}
   * for this executor to be monitored.
   *
   * @param mode describes the group the calling thread should be associated with.
   * @return the created {@link AbstractExecutor} instance.
   */
  public AbstractExecutor createAbstractExecutor(Mode mode);

  /**
   * Call to cause this thread monitor to start monitoring
   * the given executor.
   *
   * @param executor the executor to monitor.
   * @return true - if succeeded , false - if failed.
   */
  public boolean register(AbstractExecutor executor);

  /**
   * Call to cause this thread monitor to stop monitoring
   * the given executor.
   *
   * @param executor the executor to stop monitoring.
   */
  public void unregister(AbstractExecutor executor);

  /**
   * A long-running thread that may appear stuck should periodically update its "alive"
   * status by invoking this method
   */
  public void updateThreadStatus();
}
