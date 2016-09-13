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
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * Represents a server. Keeps track of information about the specific server
 * @since GemFire 5.7
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
    return closed;
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
