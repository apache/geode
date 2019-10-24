/*
 *
 * * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * * agreements. See the NOTICE file distributed with this work for additional information regarding
 * * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the
 * * "License"); you may not use this file except in compliance with the License. You may obtain a
 * * copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software distributed under the
 * License
 * * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express
 * * or implied. See the License for the specific language governing permissions and limitations
 * under
 * * the License.
 *
 */

package org.apache.geode.internal.logging;

import java.util.Map;


public interface ThreadsMonitoring {

  public enum Mode {
    FunctionExecutor,
    PooledExecutor,
    SerialQueuedExecutor,
    OneTaskOnlyExecutor,
    ScheduledThreadExecutor,
    AGSExecutor
  };

  Map<Long, AbstractExecutor> getMonitorMap();

  /**
   * Closes this ThreadMonitoring and releases all resources associated with it.
   */
  void close();

  /**
   * Starting to monitor a new executor object.
   *
   * @param mode the object executor group.
   * @return true - if succeeded , false - if failed.
   */
  public boolean startMonitor(Mode mode);

  /**
   * Ending the monitoring of an executor object.
   */
  public void endMonitor();

  /**
   * A long-running thread that may appear stuck should periodically update its "alive"
   * status by invoking this method
   */
  public void updateThreadStatus();
}
