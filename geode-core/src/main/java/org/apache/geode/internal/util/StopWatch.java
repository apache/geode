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
package org.apache.geode.internal.util;


/** Stop watch for measuring elapsed time. Not thread-safe. */
public class StopWatch {

  private long startTime;
  private long stopTime;

  /** Constructs a StopWatch which has not yet been started */
  public StopWatch() {
    this(false);
  }

  /** @param start true if new StopWatch should automatically start */
  public StopWatch(boolean start) {
    if (start) {
      start();
    }
  }

  /**
   * Returns the elapsed time in millis since starting. Value is final once stopped.
   */
  public long elapsedTimeMillis() {
    if (stopTime == 0) {
      return System.currentTimeMillis() - startTime;
    } else {
      return stopTime - startTime;
    }
  }

  /** Start the stop watch */
  public void start() {
    startTime = System.currentTimeMillis();
    stopTime = 0;
  }

  /** Stop the stop watch */
  public void stop() {
    if (!isRunning()) {
      throw new IllegalStateException(
          "Attempted to stop non-running StopWatch");
    }
    stopTime = System.currentTimeMillis();
  }

  /** Returns true if stop watch is currently running */
  public boolean isRunning() {
    return startTime > 0 && stopTime == 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("[StopWatch: ");
    sb.append("startTime=").append(startTime);
    sb.append(", stopTime=").append(stopTime);
    sb.append(", isRunning=").append(isRunning());
    sb.append("]");
    return sb.toString();
  }
}
