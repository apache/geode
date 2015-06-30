/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. GoPivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * Custom eviction attributes including {@link EvictionCriteria} and evictor
 * start time and frequency, if any.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public abstract class CustomEvictionAttributes {

  private final EvictionCriteria<?, ?> criteria;

  private final long evictorStartTime;
  private final long evictorInterval;

  private final boolean evictIncoming;

  protected CustomEvictionAttributes(EvictionCriteria<?, ?> criteria,
      long startTime, long interval, boolean evictIncoming) {
    this.criteria = criteria;
    this.evictorStartTime = startTime;
    this.evictorInterval = interval;
    this.evictIncoming = evictIncoming;
  }

  /**
   * Get the {@link EvictionCriteria} for this custom eviction. The criteria
   * will be applied to the region entries either periodically as per
   * {@link #getEvictorStartTime()} and {@link #getEvictorInterval()}, or on
   * incoming puts if {@link #isEvictIncoming()} is true.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <K, V> EvictionCriteria<K, V> getCriteria() {
    return (EvictionCriteria)this.criteria;
  }

  /**
   * The absolute start time in milliseconds (as returned by
   * {@link System#currentTimeMillis()}) when the evictor will be first fired.
   * Thereafter the evictor will be fired periodically every
   * {@link #getEvictorInterval()} milliseconds.
   */
  public final long getEvictorStartTime() {
    return this.evictorStartTime;
  }

  /**
   * The intervals at which the periodic evictor task is fired and
   * {@link EvictionCriteria} evaluated to evict entries.
   */
  public final long getEvictorInterval() {
    return this.evictorInterval;
  }

  /**
   * If this returns true, then the criteria should always be applied to
   * incoming entries and never as a periodic task.
   */
  public final boolean isEvictIncoming() {
    return this.evictIncoming;
  }
}
