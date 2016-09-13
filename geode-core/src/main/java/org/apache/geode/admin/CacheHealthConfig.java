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
package com.gemstone.gemfire.admin;

/**
 * Provides configuration information relating to the health of a
 * member of a GemFire distributed system that hosts a GemFire {@link
 * com.gemstone.gemfire.cache.Cache Cache}.
 *
 * <P>
 *
 * If any of the following criteria is true, then a cache member is
 * considered to be in {@link GemFireHealth#OKAY_HEALTH OKAY_HEALTH}.
 *
 * <UL>
 *
 * <LI><code>netSearch</code> operations take {@linkplain
 * #getMaxNetSearchTime too long} to complete.</LI>
 *
 * <LI>Cache <code>load</code> operations take {@linkplain
 * #getMaxLoadTime too long} to complete.</LI>
 *
 * <LI>The overall cache {@link #getMinHitRatio hitRatio} is too
 * small</LI> 
 *
 * <LI>The number of entries in the Cache {@link #getMaxEventQueueSize
 * event delivery queue} is too large.</LI>
 * 
 * <LI>If one of the regions is configured with {@link com.gemstone.gemfire.cache.LossAction#FULL_ACCESS FULL_ACCESS}
 * on role loss.</LI>
 *
 * </UL>
 *
 * If any of the following criteria is true, then a cache member is
 * considered to be in {@link GemFireHealth#POOR_HEALTH POOR_HEALTH}.
 * 
 * <UL>
 * 
 * <LI>If one of the regions is configured with {@link com.gemstone.gemfire.cache.LossAction#NO_ACCESS NO_ACCESS}
 * on role loss.</LI> 
 * 
 * <LI>If one of the regions is configured with {@link com.gemstone.gemfire.cache.LossAction#LIMITED_ACCESS LIMITED_ACCESS}
 * on role loss.</LI> 
 * 
 * </UL>
 * 
 * <UL>
 *
 * </UL>
 *
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 * */
public interface CacheHealthConfig {

  /** The default maximum number of milliseconds a
   * <code>netSearch</code> operation can take before the cache member
   * is considered to be unhealthy. */
  public static final long DEFAULT_MAX_NET_SEARCH_TIME = 60 * 1000;

  /** The default maximum mumber of milliseconds a cache
   * <code>load</code> operation can take before the cache member is
   * considered to be unhealthy. */
  public static final long DEFAULT_MAX_LOAD_TIME = 60 * 1000;

  /** The default minimum hit ratio of a healthy cache member. */
  public static final double DEFAULT_MIN_HIT_RATIO = 0.0;

  /** The default maximum number of entries in the event delivery queue
   * of a healthy cache member. */
  public static final long DEFAULT_MAX_EVENT_QUEUE_SIZE = 1000;

  ///////////////////////  Instance Methods  ///////////////////////
  
  /**
   * Returns the maximum number of milliseconds a
   * <code>netSearch</code> operation can take before the cache member
   * is considered to be unhealthy.
   *
   * @see #DEFAULT_MAX_NET_SEARCH_TIME
   */
  public long getMaxNetSearchTime();

  /**
   * Sets the maximum number of milliseconds a
   * <code>netSearch</code> operation can take before the cache member
   * is considered to be unhealthy.
   *
   * @see #getMaxNetSearchTime
   */
  public void setMaxNetSearchTime(long maxNetSearchTime);

  /**
   * Returns the maximum mumber of milliseconds a cache
   * <code>load</code> operation can take before the cache member is
   * considered to be unhealthy.
   *
   * @see #DEFAULT_MAX_LOAD_TIME
   */
  public long getMaxLoadTime();

  /**
   * Sets the maximum mumber of milliseconds a cache
   * <code>load</code> operation can take before the cache member is
   * considered to be unhealthy.
   *
   * @see #getMaxLoadTime
   */
  public void setMaxLoadTime(long maxLoadTime);

  /**
   * Returns the minimum hit ratio of a healthy cache member.
   *
   * @see #DEFAULT_MIN_HIT_RATIO
   */
  public double getMinHitRatio();

  /**
   * Sets the minimum hit ratio of a healthy cache member.
   *
   * @see #getMinHitRatio
   */
  public void setMinHitRatio(double minHitRatio);

  /**
   * Returns the maximum number of entries in the event delivery queue
   * of a healthy cache member.
   *
   * @see #DEFAULT_MAX_EVENT_QUEUE_SIZE
   */
  public long getMaxEventQueueSize();

  /**
   * Sets the maximum number of entries in the event delivery queue
   * of a healthy cache member.
   *
   * @see #getMaxEventQueueSize
   */
  public void setMaxEventQueueSize(long maxEventQueueSize);

}
