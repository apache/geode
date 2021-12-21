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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.CacheHealthConfig;

/**
 * The implementation of <code>CacheHealthConfig</code>
 *
 *
 * @since GemFire 3.5
 */
public abstract class CacheHealthConfigImpl extends MemberHealthConfigImpl
    implements CacheHealthConfig {

  /**
   * The maximum number of milliseconds a <code>netSearch</code> operation can take before the cache
   * member is considered to be unhealthy.
   */
  private long maxNetSearchTime = DEFAULT_MAX_NET_SEARCH_TIME;

  /**
   * The maximum mumber of milliseconds a cache <code>load</code> operation can take before the
   * cache member is considered to be unhealthy.
   */
  private long maxLoadTime = DEFAULT_MAX_LOAD_TIME;

  /** The minimum hit ratio of a healthy cache member. */
  private double minHitRatio = DEFAULT_MIN_HIT_RATIO;

  /**
   * The maximum number of entries in the event delivery queue of a healthy cache member.
   */
  private long maxEventQueueSize = DEFAULT_MAX_EVENT_QUEUE_SIZE;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>CacheHealthConfigImpl</code> with the default configuration.
   */
  CacheHealthConfigImpl() {

  }

  ////////////////////// Instance Methods /////////////////////

  @Override
  public long getMaxNetSearchTime() {
    return maxNetSearchTime;
  }

  @Override
  public void setMaxNetSearchTime(long maxNetSearchTime) {
    this.maxNetSearchTime = maxNetSearchTime;
  }

  @Override
  public long getMaxLoadTime() {
    return maxLoadTime;
  }

  @Override
  public void setMaxLoadTime(long maxLoadTime) {
    this.maxLoadTime = maxLoadTime;
  }

  @Override
  public double getMinHitRatio() {
    return minHitRatio;
  }

  @Override
  public void setMinHitRatio(double minHitRatio) {
    this.minHitRatio = minHitRatio;
  }

  @Override
  public long getMaxEventQueueSize() {
    return maxEventQueueSize;
  }

  @Override
  public void setMaxEventQueueSize(long maxEventQueueSize) {
    this.maxEventQueueSize = maxEventQueueSize;
  }

}
