/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;

/**
 * Implementation of {@link ReliableMessageQueueFactory}
 * 
 * @author Darrel Schneider
 * @since 5.0
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
   * @since 5.0
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
