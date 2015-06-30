/*
 * ========================================================================= 
 * (c)Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds.MemoryState;

/**
 * @author sbawaska
 * @author David Hoots
 */
public class MemoryEvent implements ResourceEvent {
  private final ResourceType type;
  private volatile MemoryState state;
  private final MemoryState previousState;
  private final DistributedMember member;
  private final long bytesUsed;
  private final boolean isLocal;
  private final MemoryThresholds thresholds;
  private final long eventTime;
 
  public MemoryEvent(final ResourceType type, final MemoryState previousState, final MemoryState state,
      final DistributedMember member, final long bytesUsed, final boolean isLocal, final MemoryThresholds thresholds) {
    this.type = type;
    this.previousState = previousState;
    this.state = state;
    this.member = member;
    this.bytesUsed = bytesUsed;
    this.isLocal = isLocal;
    this.thresholds = thresholds;
    this.eventTime = System.currentTimeMillis();
  }

  @Override
  public ResourceType getType() {
    return this.type;
  }
  
  public MemoryState getPreviousState() {
    return this.previousState;
  }
  
  public MemoryState getState() {
    return this.state;
  }

  @Override
  public DistributedMember getMember() {
    return this.member;
  }

  public long getBytesUsed() {
    return this.bytesUsed;
  }
  
  @Override
  public boolean isLocal() {
    return this.isLocal;
  }
  
  public long getEventTime() {
    return this.eventTime;
  }

  public MemoryThresholds getThresholds() {
    return this.thresholds;
  }
  
  @Override
  public String toString() {
    return new StringBuilder().append("MemoryEvent@")
        .append(System.identityHashCode(this))
        .append("[Member:" + this.member)
        .append(",type:" + this.type)
        .append(",previousState:" + this.previousState)
        .append(",state:" + this.state)
        .append(",bytesUsed:" + this.bytesUsed)
        .append(",isLocal:" + this.isLocal)
        .append(",eventTime:" + this.eventTime)
        .append(",thresholds:" + this.thresholds + "]")
        .toString();
  }
}

