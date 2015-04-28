/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * Represents a server. Keeps track of information about the specific server
 * @author dsmith
 * @since 5.7
 *
 */
public class Endpoint {
  
  private AtomicLong lastExecute = new AtomicLong();
  private AtomicInteger references = new AtomicInteger();
  private final ServerLocation location;
  private final ConnectionStats stats;
  private final EndpointManagerImpl manager;
  private final DistributedMember memberId;
  private volatile boolean closed;
  
  Endpoint(EndpointManagerImpl endpointManager, DistributedSystem ds,
      ServerLocation location, ConnectionStats stats,
      DistributedMember memberId) {
    this.manager =endpointManager;
    this.location = location;
    this.stats = stats;
    this.memberId = memberId;
    updateLastExecute();
  }

  public void updateLastExecute() {
    this.lastExecute.set(System.nanoTime());
  }

  private long getLastExecute() {
    return lastExecute.get();
  }

  public boolean timeToPing(long pingIntervalNanos) {
    long now = System.nanoTime();
    return getLastExecute() <= (now - pingIntervalNanos);
  }
  
  public void close() {
    if(!closed) {
      closed = true;
    }
  }
  
  public boolean isClosed() {
    return closed == true;
  }

  public ConnectionStats getStats() {
    return stats;
  }

  void addReference() {
    references.incrementAndGet();
  }

  /**
   * @return true if this was the last reference to the endpoint
   */
  public boolean removeReference() {
    boolean lastReference = (references.decrementAndGet() <= 0);
    if(lastReference) {
      manager.endpointNotInUse(this);
    }
    return lastReference;
  }

  /**
   * @return the location of this server
   */
  public ServerLocation getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return location.toString();
  }
  
  public DistributedMember getMemberId() {
    return memberId;
  }
  
  
}
