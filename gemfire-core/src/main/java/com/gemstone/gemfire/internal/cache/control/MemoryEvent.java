/*
 * ========================================================================= 
 * (c)Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.Thresholds;
/**
 * @author sbawaska
 *
 */
public interface MemoryEvent extends ResourceEvent<MemoryEventType> {
  /**
   * @return the member where the event took place
   */
  public DistributedMember getMember();
  /**
   * @return current percentage of tenured/old generation used.
   */
  public int getCurrentHeapUsagePercent();
  /**
   * @return current number of used bytes in tenured/old generation.
   */
  public long getCurrentHeapBytesUsed();

  /**
   * Gets the difference between threshold and the current bytes used.
   * For UP events, it is the bytes above threshold, for DOWN events, it is the bytes below
   * threshold. For DISABLE events returns zero.
   * @return the difference in bytes from threshold
   */
  public long getBytesFromThreshold();

  /**
   * Determine if the event's origin is local
   * @return true if local otherwise false
   */
  public boolean isLocal();

  /**
   * Get the memory thresholds the Resource Manager was configured with
   * when the event was fired.  Memory thresholds are mutable and they may
   * change at any time which makes using {@link ResourceManager#getCriticalHeapPercentage()}
   * {@link ResourceManager#getEvictionHeapPercentage()} a risk with respect to the
   * conditions that fired this event.
   * @return the threshold configuration for this event
   */
  public Thresholds getThresholds();
}

