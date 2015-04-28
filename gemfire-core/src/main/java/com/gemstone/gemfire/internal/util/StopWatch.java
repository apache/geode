/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

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
   * Returns the elapsed time in millis since starting. Value is final once 
   * stopped. 
   */
  public long elapsedTimeMillis() {
    if (this.stopTime == 0) {
      return System.currentTimeMillis() - this.startTime;
    }
    else {
      return this.stopTime - this.startTime;
    }
  }

  /** Start the stop watch */
  public void start() {
    this.startTime = System.currentTimeMillis();
    this.stopTime = 0;
  }
    
  /** Stop the stop watch */
  public void stop() {
    if (!isRunning()) {
      throw new IllegalStateException(LocalizedStrings.StopWatch_ATTEMPTED_TO_STOP_NONRUNNING_STOPWATCH.toLocalizedString());
    }
    this.stopTime = System.currentTimeMillis();
  }
  
  /** Returns true if stop watch is currently running */
  public boolean isRunning() {
    return this.startTime > 0 && this.stopTime == 0;
  }
    
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("[StopWatch: ");
    sb.append("startTime=").append(this.startTime);
    sb.append(", stopTime=").append(this.stopTime);
    sb.append(", isRunning=").append(isRunning());
    sb.append("]");
    return sb.toString();
  }
}
