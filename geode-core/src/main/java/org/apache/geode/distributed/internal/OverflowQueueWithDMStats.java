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

package org.apache.geode.distributed.internal;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * A LinkedBlockingQueue that supports stats. Named OverflowQueue for historical reasons.
 *
 */
public class OverflowQueueWithDMStats<E> extends LinkedBlockingQueue<E> {
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
  public boolean add(E e) {
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
  public boolean offer(E e) {
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
  public void put(E e) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
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
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
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
  public E take() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    E result = super.take();
    postRemove(result);
    this.stats.remove();
    return result;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    E result = super.poll(timeout, unit);
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
  public int drainTo(Collection<? super E> c) {
    int result = super.drainTo(c);
    if (result > 0) {
      this.stats.remove(result);
      postDrain(c);
    }
    return result;
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
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
  protected void preAddInterruptibly(E o) throws InterruptedException {
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
   * Called after the specified collection of objects have been drained (i.e. removed) from this
   * queue.
   */
  protected void postDrain(Collection<? super E> c) {
    // do nothing in this class. sub-classes can override
  }

  @VisibleForTesting
  public QueueStatHelper getQueueStatHelper() {
    return stats;
  }
}
