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
public class MemoryEventImpl implements MemoryEvent {

  public static final MemoryEventImpl UNKOWN = new MemoryEventImpl(MemoryEventType.UNKNOWN, null, 0, 0, 0, true, new Thresholds());
  public static final MemoryEventImpl NO_DELIVERY = new MemoryEventImpl(MemoryEventType.UNKNOWN, null, 0, 0, 0, true, new Thresholds());
  private final MemoryEventType type;
  private final DistributedMember member;
  private final int currentHeapUsagePercent;
  private final long currentHeapBytesUsed;
  private final boolean isLocal;
  private final long bytesFromThreshold;
  private final Thresholds thresholds;
  private final boolean skipValidation;

  public MemoryEventImpl(MemoryEventType type,
      DistributedMember member,
      int currentHeapUsagePercent,
      long currentHeapBytesUsed,
      long bytesFromThreshold,
      boolean isLocal, Thresholds t) {
    assert type != null;
    this.type = type;
    this.member = member;
    this.currentHeapUsagePercent = currentHeapUsagePercent;
    this.currentHeapBytesUsed = currentHeapBytesUsed;
    this.bytesFromThreshold = bytesFromThreshold;
    this.isLocal = isLocal;
    assert t != null;
    this.thresholds = t;
    this.skipValidation = false;
  }

  /**
   * Constructs a new MemoryEventImpl with all values same as the given event
   * but the MemoryEventType overridden with overrideType
   * @param event
   * @param overrideType
   */
  public MemoryEventImpl(MemoryEventImpl event,
      MemoryEventType overrideType) {
    assert overrideType != null;
    this.type = overrideType;
    this.member = event.member;
    this.currentHeapUsagePercent = event.currentHeapUsagePercent;
    this.currentHeapBytesUsed = event.currentHeapBytesUsed;
    this.bytesFromThreshold = event.bytesFromThreshold;
    this.isLocal = event.isLocal;
    this.thresholds = event.thresholds;
    this.skipValidation = event.skipValidation;
  }

  /**
   * Constructor for skipping validations. Used from
   * {@link ResourceManager#setCriticalHeapPercentage(float)} and
   * {@link ResourceManager#setEvictionHeapPercentage(float)}
   * @param type
   * @param currentHeapBytesUsed
   * @param member
   * @param isLocal
   * @param t
   * @param forcedEvent
   */
  public MemoryEventImpl(MemoryEventType type, long currentHeapBytesUsed,
      DistributedMember member, boolean isLocal, Thresholds t, boolean forcedEvent) {
    this.type = type;
    this.member = member;
    this.currentHeapUsagePercent = 0;
    this.currentHeapBytesUsed = currentHeapBytesUsed;
    this.bytesFromThreshold = 0;
    this.isLocal = isLocal;
    this.thresholds = t;
    this.skipValidation = forcedEvent;
  }
  
  /**
   * Constructor for TEST EVENTS ONLY
   * @param currentHeapBytesUsed
   * @param member
   * @param isLocal
   */
  public MemoryEventImpl(long currentHeapBytesUsed, 
      DistributedMember member, boolean isLocal, Thresholds t) {
    this.type = MemoryEventType.UNKNOWN;
    this.member = member;
    this.currentHeapUsagePercent = 0;
    this.currentHeapBytesUsed = currentHeapBytesUsed;
    this.bytesFromThreshold = 0;
    this.isLocal = isLocal;
    this.thresholds = t;
    this.skipValidation = false;
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.MemoryEvent#getCurrentHeapUsagePercent()
   */
  public int getCurrentHeapUsagePercent() {
    return currentHeapUsagePercent;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.MemoryEvent#getMember()
   */
  public DistributedMember getMember() {
    return member;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.MemoryEvent#getType()
   */
  public MemoryEventType getType() {
    return type;
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.MemoryEvent#isLocal()
   */
  public boolean isLocal() {
    return isLocal;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new StringBuilder()
      .append("MemoryEvent@").append(System.identityHashCode(this))
      .append("[Member:"+member)
      .append(",eventType:"+type)
      .append(",currentHeapUsagePercent:"+currentHeapUsagePercent)
      .append(",bytesFromThreshold:"+bytesFromThreshold)
      .append(",currentHeapBytesUsed:"+currentHeapBytesUsed)
      .append(",isLocal:"+isLocal)
      .append(",skipValidation:"+skipValidation+"]")
      .toString();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.control.MemoryEvent#getBytesFromThreshold()
   */
  public long getBytesFromThreshold() {
    return bytesFromThreshold;
  }
  
  public boolean isDisableEvent() {
    assert this.type != null;
    return type.isDisabledType();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.control.MemoryEvent#getCurrentHeapBytesUsed()
   */
  public long getCurrentHeapBytesUsed() {
    return currentHeapBytesUsed;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.control.MemoryEvent#getThresholds()
   */
  public Thresholds getThresholds() {
    return this.thresholds;
  }
  
  /**
   * @return true if this is a special event that is delivered irrespective 
   * of the state machine, false otherwise
   */
  public boolean skipValidation() {
    return skipValidation;
  }
}
