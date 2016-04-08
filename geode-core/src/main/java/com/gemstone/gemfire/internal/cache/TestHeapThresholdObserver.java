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
package com.gemstone.gemfire.internal.cache;

/**
 * Test only hand back object passed to
 * 
 * 
 */
public class TestHeapThresholdObserver {

  private int threshold;

  private long maxMemory;

  private long thresholdInBytes;

  private long delta;

  public TestHeapThresholdObserver(int threshold) {
    this.threshold = threshold;
  }

  public long getDelta() {
    return delta;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public int getThreshold() {
    return threshold;
  }

  public long getThresholdInBytes() {
    return thresholdInBytes;
  }

  public void setNotificationInfo(int threshold, long maxMemory,
      long thresholdInBytes, long delta) {
    this.threshold = threshold;
    this.maxMemory = maxMemory;
    this.thresholdInBytes = thresholdInBytes;
    this.delta = delta;
  }
}
