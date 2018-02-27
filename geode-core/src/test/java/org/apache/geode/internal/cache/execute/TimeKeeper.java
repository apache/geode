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
package org.apache.geode.internal.cache.execute;

/**
 * Extracted from {@link PRPerformanceTestDUnitTest}.
 */
class TimeKeeper {

  private long startTime = -1;
  private long endTime = -1;

  public void start() {
    startTime = System.currentTimeMillis();
  }

  public void stop() {
    endTime = System.currentTimeMillis();
  }

  public long getTimeInMs() {
    if (startTime == -1 || endTime == -1) {
      return -1;
    } else if (endTime == startTime) {
      return 1;
    } else
      return endTime - startTime;
  }
}
