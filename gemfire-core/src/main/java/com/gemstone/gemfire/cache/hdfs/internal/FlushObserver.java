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
package com.gemstone.gemfire.cache.hdfs.internal;

import java.util.concurrent.TimeUnit;

/**
 * Observes and reacts to flush events.
 * 
 * @author bakera
 */
public interface FlushObserver {
  public interface AsyncFlushResult {
    /**
     * Waits for the most recently enqueued batch to completely flush.
     * 
     * @param time the time to wait
     * @param unit the time unit
     * @return true if flushed before the timeout
     * @throws InterruptedException interrupted while waiting
     */
    public boolean waitForFlush(long time, TimeUnit unit) throws InterruptedException;
  }

  /**
   * Returns true when the queued events should be drained from the queue
   * immediately.
   * 
   * @return true if draining
   */
  boolean shouldDrainImmediately();
  
  /**
   * Begins the flushing the queued events.
   * 
   * @return the async result
   */
  public AsyncFlushResult flush();
}

