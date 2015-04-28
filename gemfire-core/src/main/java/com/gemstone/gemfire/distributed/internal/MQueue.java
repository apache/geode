package com.gemstone.gemfire.distributed.internal;

import java.util.concurrent.BlockingQueue;

/**
 * The <code>MQueue</code> interface allows us to have multiple
 * implementations of the distribution manager's message queue.
 */
public interface MQueue extends BlockingQueue {

  /**
   * Signals the semaphore on which this queue is waiting.
   */
  public void wakeUp();

}
