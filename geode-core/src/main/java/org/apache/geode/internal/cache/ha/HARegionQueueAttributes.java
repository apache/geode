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
package org.apache.geode.internal.cache.ha;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 *
 * This class defines the user specified attributes of the HARegion which are configurable.
 *
 *
 */
public class HARegionQueueAttributes {
  /**
   * default expiry time for region entries in seconds
   */

  private static final int DEFAULT_EXPIRY_TIME = 180;
  private static final int DEFAULT_BLOCKING_QUEUE_CAPACITY = 230000;

  /**
   * String storing the System Property key representing the blocking queue capacity
   */
  private static final String BLOCKING_QUEUE_CAPACITY =
      GeodeGlossary.GEMFIRE_PREFIX + "Capacity";

  /**
   * expiry time for region entries in seconds
   */

  private int expiryTime = DEFAULT_EXPIRY_TIME;

  private int blockingQueueCapacity =
      Integer.getInteger(BLOCKING_QUEUE_CAPACITY, DEFAULT_BLOCKING_QUEUE_CAPACITY).intValue();

  // TODO:Asif: We shoudl prevent modification of this object by using
  // HARegionAttributesFactory instead of directly
  // providing getter/setter in HARegionAttributes. HAregionAttributes should be
  // immutable
  @Immutable
  static final HARegionQueueAttributes DEFAULT_HARQ_ATTRIBUTES = new HARegionQueueAttributes();

  /**
   * Default constructor
   */
  public HARegionQueueAttributes() {
    // this.blockingQueueCapacity =
    // Integer.getInteger(BLOCKING_QUEUE_CAPACITY,DEFAULT_BLOCKING_QUEUE_CAPACITY).intValue();
  }

  /**
   * Gets the expiration time for the region entries
   *
   * @return the expiry time in seconds
   */
  public int getExpiryTime() {
    return expiryTime;
  }

  /**
   * Sets the expiration time for the region entries
   *
   * @param expiryTime expiry time in seconds
   */
  public void setExpiryTime(int expiryTime) {
    this.expiryTime = expiryTime;
  }

  /**
   * Gets the blocking queue capacity
   *
   * @return the blocking queue capacity
   */
  public int getBlockingQueueCapacity() {
    return blockingQueueCapacity;
  }

  /**
   * Sets the capacity of the queue
   *
   * @param cap number of items allowed in the queue
   */
  public void setBlockingQueueCapacity(int cap) {
    blockingQueueCapacity = cap;
  }

}
