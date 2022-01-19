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
package org.apache.geode.cache.client.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * Represents a server. Keeps track of information about the specific server
 *
 * @since GemFire 5.7
 *
 */
public class Endpoint {

  private final AtomicLong lastExecute = new AtomicLong();
  private final AtomicInteger references = new AtomicInteger();
  private final ServerLocation location;
  private final ConnectionStats stats;
  private final EndpointManagerImpl manager;
  private final DistributedMember memberId;
  private volatile boolean closed;

  Endpoint(EndpointManagerImpl endpointManager, DistributedSystem ds, ServerLocation location,
      ConnectionStats stats, DistributedMember memberId) {
    manager = endpointManager;
    this.location = location;
    this.stats = stats;
    this.memberId = memberId;
    updateLastExecute();
  }

  public void updateLastExecute() {
    lastExecute.set(System.nanoTime());
  }

  private long getLastExecute() {
    return lastExecute.get();
  }

  public boolean timeToPing(long pingIntervalNanos) {
    long now = System.nanoTime();
    return getLastExecute() <= (now - pingIntervalNanos);
  }

  public void close() {
    if (!closed) {
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
    if (lastReference) {
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
    return location.toString() + "," + getMemberId();
  }

  public DistributedMember getMemberId() {
    return memberId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Endpoint)) {
      return false;
    }
    final Endpoint other = (Endpoint) obj;

    if (!location.equals(other.getLocation())) {
      return false;
    }

    return memberId.equals(other.getMemberId());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + location.hashCode() + memberId.hashCode();
    return result;
  }


}
