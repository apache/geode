/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Collection;

/**
 * A LinkedBlockingQueue that supports stats.
 * Named OverflowQueue for historical reasons.
 * @author darrel
 *
 */
public class OverflowQueueWithDMStats extends LinkedBlockingQueue {
  private static final long serialVersionUID = -1846248853494394996L;
  protected final QueueStatHelper stats;
  
  /** Creates new OverflowQueueWithDMStats */
  public OverflowQueueWithDMStats(QueueStatHelper stats) {
    super();
    this.stats = stats;
  }
  /** Creates new OverflowQueueWithDMStats with a maximum capacity. */
  public OverflowQueueWithDMStats(int capacity, QueueStatHelper stats) {
    super(capacity);
    this.stats = stats;
  }
  
  @Override
  public boolean add(Object e) {
    preAdd(e);
    if (super.add(e)) {
      this.stats.add();
      return true;
    } else {
      postRemove(e);
      return false;
    }
  }
  @Override
  public boolean offer(Object e) {
    preAdd(e);
    if (super.offer(e)) {
      this.stats.add();
      return true;
    } else {
      postRemove(e);
      return false;
    }
  }
  @Override
  public void put(Object e) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    preAddInterruptibly(e);
    boolean didOp = false;
    try {
      super.put(e);
      didOp = true;
      this.stats.add();
    } finally {
      if (!didOp) {
        postRemove(e);
      }
    }
  }
  @Override
  public boolean offer(Object e, long timeout, TimeUnit unit)
    throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    preAddInterruptibly(e);
    boolean didOp = false;
    try {
      if (super.offer(e, timeout, unit)) {
        didOp = true;
        this.stats.add();
        return true;
      } else {
        return false;
      }
    } finally {
      if (!didOp) {
        postRemove(e);
      }
    }
  }
  @Override
  public Object take() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    Object result = super.take();
    postRemove(result);
    this.stats.remove();
    return result;
  }
  @Override
  public Object poll(long timeout, TimeUnit unit)
    throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    Object result = super.poll(timeout, unit);
    if (result != null) {
      postRemove(result);
      this.stats.remove();
    }
    return result;
  }
  @Override
  public boolean remove(Object o) {
    if (super.remove(o)) {
      this.stats.remove();
      postRemove(o);
      return true;
    } else {
      return false;
    }
  }
  @Override
  public int drainTo(Collection c) {
    int result = super.drainTo(c);
    if (result > 0) {
      this.stats.remove(result);
      postDrain(c);
    }
    return result;
  }
  @Override
  public int drainTo(Collection c, int maxElements) {
    int result = super.drainTo(c, maxElements);
    if (result > 0) {
      this.stats.remove(result);
      postDrain(c);
    }
    return result;
  }
  /**
   * Called before the specified object is added to this queue.
   */
  protected void preAddInterruptibly(Object o) throws InterruptedException {
    // do nothing in this class. sub-classes can override
  }
  /**
   * Called before the specified object is added to this queue.
   */
  protected void preAdd(Object o) {
    // do nothing in this class. sub-classes can override
  }
  /**
   * Called after the specified object is removed from this queue.
   */
  protected void postRemove(Object o) {
    // do nothing in this class. sub-classes can override
  }
  /**
   * Called after the specified collection of objects have been
   * drained (i.e. removed) from this queue.
   */
  protected void postDrain(Collection c) {
    // do nothing in this class. sub-classes can override
  }
}
