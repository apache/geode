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
package org.apache.geode.internal.cache.partitioned;

import org.apache.geode.internal.cache.PartitionedRegionHelper;

/**
 * A Simple class used to track retry time for Region operations Does not provide any
 * synchronization or concurrent safety
 */
public class RetryTimeKeeper {
  private long totalTimeInRetry;

  private final long maxTimeInRetry;

  @FunctionalInterface
  public interface Factory {
    RetryTimeKeeper create(int maxTime);
  }

  public RetryTimeKeeper(int maxTime) {
    this.maxTimeInRetry = maxTime;
  }

  /**
   * wait for {@link PartitionedRegionHelper#DEFAULT_WAIT_PER_RETRY_ITERATION}, updating the total
   * wait time. Use this method when the same node has been selected for consecutive attempts with
   * an operation.
   */
  public void waitToRetryNode() {
    this.waitForBucketsRecovery();
  }

  /**
   * Wait for {@link PartitionedRegionHelper#DEFAULT_WAIT_PER_RETRY_ITERATION} time and update the
   * total wait time.
   */
  public void waitForBucketsRecovery() {
    /*
     * Unfortunately, due to interrupts plus the vagaries of thread scheduling, we can't assume
     * that our sleep is for exactly the amount of time that we specify. Thus, we need to measure
     * the before/after times and increment the counter accordingly.
     */
    long start = System.currentTimeMillis();
    boolean interrupted = Thread.interrupted();
    try {
      Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
    } catch (InterruptedException ignore) {
      interrupted = true;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    long delta = System.currentTimeMillis() - start;
    if (delta < 1) {
      // I don't think this can happen, but I want to guarantee that
      // this thing will eventually time out.
      delta = 1;
    }
    this.totalTimeInRetry += delta;
  }

  public boolean overMaximum() {
    return this.totalTimeInRetry > this.maxTimeInRetry;
  }

  public long getRetryTime() {
    return this.totalTimeInRetry;
  }
}
