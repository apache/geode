/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;

/**
 * The implementation of <code>CacheHealthConfig</code>
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public abstract class CacheHealthConfigImpl
  extends MemberHealthConfigImpl implements CacheHealthConfig {

  /** The maximum number of milliseconds a
   * <code>netSearch</code> operation can take before the cache member
   * is considered to be unhealthy. */
  private long maxNetSearchTime = DEFAULT_MAX_NET_SEARCH_TIME;

  /** The maximum mumber of milliseconds a cache
   * <code>load</code> operation can take before the cache member is
   * considered to be unhealthy. */
  private long maxLoadTime = DEFAULT_MAX_LOAD_TIME;

  /** The minimum hit ratio of a healthy cache member. */
  private double minHitRatio = DEFAULT_MIN_HIT_RATIO;

  /** The maximum number of entries in the event delivery queue
   * of a healthy cache member. */
  private long maxEventQueueSize = DEFAULT_MAX_EVENT_QUEUE_SIZE;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>CacheHealthConfigImpl</code> with the default
   * configuration.
   */
  CacheHealthConfigImpl() {

  }

  //////////////////////  Instance Methods  /////////////////////

  public long getMaxNetSearchTime() {
    return this.maxNetSearchTime;
  }

  public void setMaxNetSearchTime(long maxNetSearchTime) {
    this.maxNetSearchTime = maxNetSearchTime;
  }

  public long getMaxLoadTime() {
    return this.maxLoadTime;
  }

  public void setMaxLoadTime(long maxLoadTime) {
    this.maxLoadTime = maxLoadTime;
  }

  public double getMinHitRatio() {
    return this.minHitRatio;
  }

  public void setMinHitRatio(double minHitRatio) {
    this.minHitRatio = minHitRatio;
  }

  public long getMaxEventQueueSize() {
    return this.maxEventQueueSize;
  }

  public void setMaxEventQueueSize(long maxEventQueueSize) {
    this.maxEventQueueSize = maxEventQueueSize;
  }

}
