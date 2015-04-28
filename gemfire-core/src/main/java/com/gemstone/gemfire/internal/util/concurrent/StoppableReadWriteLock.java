package com.gemstone.gemfire.internal.util.concurrent;

import com.gemstone.gemfire.CancelCriterion;

/**
 * A non-reentrant ReadWriteLock that responds to Cancellation. The underlying lock
 * is {@link SemaphoreReadWriteLock}, which is a count based lock. 
 * 
 * @author sbawaska
 */
public class StoppableReadWriteLock extends StoppableReentrantReadWriteLock {

  private static final long serialVersionUID = 2904011593472799745L;

  public StoppableReadWriteLock(CancelCriterion stopper) {
    super(new SemaphoreReadWriteLock(), stopper);
  }
}

