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
package org.apache.geode.internal.cache;

import org.apache.geode.distributed.Role;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.util.*;

/**
 * Implementation of {@link ReliableMessageQueueFactory}
 * 
 * @since GemFire 5.0
 */
public class ReliableMessageQueueFactoryImpl implements ReliableMessageQueueFactory {
  private boolean closed;
  /**
   * Create a factory given its persistence attributes.
   */
  public ReliableMessageQueueFactoryImpl() {
    this.closed = false;
  }

  /**
   * Contains all the unclosed queues that have been created by this factory.
   */
  private final ArrayList queues = new ArrayList();
  
  public ReliableMessageQueue create(DistributedRegion region) {
    if (this.closed) {
      throw new IllegalStateException(LocalizedStrings.ReliableMessageQueueFactoryImpl_RELIABLE_MESSAGE_QUEUE_IS_CLOSED.toLocalizedString());
    }
    synchronized (this.queues) {
      Queue q = new Queue(region);
      this.queues.add(q);
      return q;
    }
  }
  
  public void close(boolean force) {
    // @todo darrel: nyi
    if (!force) {
      synchronized (this.queues) {
        if (!this.queues.isEmpty()) {
          throw new IllegalStateException(LocalizedStrings.ReliableMessageQueueFactoryImpl_REGIONS_WITH_MESSAGE_QUEUING_ALREADY_EXIST.toLocalizedString());
        }
      }
    }
    this.closed = true;
  }

  /**
   * Maps DistributedRegion keys to QueuedRegionData values
   */
  private final IdentityHashMap regionMap = new IdentityHashMap(128);

  /**
   * Adds data in the specified region to be sent to the specified roles
   */
  protected void add(DistributedRegion r, ReliableDistributionData data, Set roles) {
    QueuedRegionData qrd = null;
    synchronized(this.regionMap) {
      qrd = (QueuedRegionData)this.regionMap.get(r);
    }
    qrd.add(r, data, roles);
    r.getCachePerfStats().incReliableQueuedOps(data.getOperationCount() * roles.size());
  }
  public Set getQueuingRoles(DistributedRegion r) {
    QueuedRegionData qrd = null;
    synchronized(this.regionMap) {
      qrd = (QueuedRegionData)this.regionMap.get(r);
    }
    return qrd.getQueuingRoles(r);
  }
  protected boolean roleReady(DistributedRegion r, Role role) {
    QueuedRegionData qrd = null;
    synchronized(this.regionMap) {
      qrd = (QueuedRegionData)this.regionMap.get(r);
    }
    return qrd.roleReady(r, role);
  }
  /**
   * Initializes data queuing for the given region
   */
  protected void init(DistributedRegion r) {
    QueuedRegionData qrd = new QueuedRegionData();
    synchronized(this.regionMap) {
      Object old = this.regionMap.put(r, qrd);
      if (old != null) {
        throw new IllegalStateException(LocalizedStrings.ReliableMessageQueueFactoryImpl_UNEXPECTED_QUEUEDREGIONDATA_0_FOR_REGION_1.toLocalizedString(new Object[] {old, r}));
      }
    }
  }
  /**
   * Removes any data queued for the given region
   */
  protected void destroy(DistributedRegion r) {
    QueuedRegionData qrd = null;
    synchronized(this.regionMap) {
      qrd = (QueuedRegionData)this.regionMap.remove(r);
    }
    if (qrd != null) {
      qrd.destroy(r);
    }
  }
  /**
   * Removes a previously created queue from this factory.
   */
  protected void removeQueue(Queue q) {
    synchronized (this.queues) {
      this.queues.remove(q);
    }
  }

  /**
   * Implements ReliableMessageQueue.
   * @since GemFire 5.0
   */
  public class Queue implements ReliableMessageQueue {
    private final DistributedRegion r;
    
    Queue(DistributedRegion r) {
      this.r = r;
      init(this.r);
    }
    public DistributedRegion getRegion() {
      return this.r;
    }
    public void add(ReliableDistributionData data, Set roles) {
      ReliableMessageQueueFactoryImpl.this.add(this.r, data, roles);
    }
    public Set getQueuingRoles() {
      return ReliableMessageQueueFactoryImpl.this.getQueuingRoles(this.r);
    }
    public boolean roleReady(Role role) {
      return ReliableMessageQueueFactoryImpl.this.roleReady(this.r, role);
    }
    public void destroy() {
      ReliableMessageQueueFactoryImpl.this.destroy(this.r);
    }
    public void close() {
      removeQueue(this);
    }
  }
  /**
   * Used to organize all the queued data for a region.
   */
  static protected class QueuedRegionData {
    /**
     * Maps Role keys to lists of ReliableDistributionData
     */
    private final HashMap roleMap = new HashMap();

    /**
     * Adds data in the specified region to be sent to the specified roles
     */
    protected void add(DistributedRegion r, ReliableDistributionData data, Set roles) {
      synchronized (this) {
        Iterator it = roles.iterator();
        while (it.hasNext()) {
          Role role = (Role)it.next();
          List l = (List)this.roleMap.get(role);
          if (l == null) {
            l = new ArrayList();
            this.roleMap.put(role, l);
          }
          l.addAll(data.getOperations());
        }
      }
    }
    
    protected Set getQueuingRoles(DistributedRegion r) {
      Set result = null;
      synchronized (this) {
        Iterator it = this.roleMap.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry me = (Map.Entry)it.next();
          List l = (List)me.getValue();
          if (l != null && !l.isEmpty()) {
            // found a role with a non-empty list of operations so add to result
            if (result == null) {
              result = new HashSet();
            }
            result.add(me.getKey());
          }
        }
      }
      return result;
    }
    
    protected boolean roleReady(DistributedRegion r, Role role) {
      List l = null;
      synchronized (this) {
        l = (List)this.roleMap.get(role);
      }
      if (l != null) {
        // @todo darrel: do this in a background thread
        while (!l.isEmpty()) {
          if (!r.sendQueue(l, role)) {
            // Couldn't send the last message so stop and return false
            return false;
          }
        }
      }
      return true;
    }
    
    /**
     * Blows away all the data in this object.
     */
    public void destroy(DistributedRegion r) {
      // @todo darrel: nothing needs doing until we use disk
    }
  }
}
