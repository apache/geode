package com.gemstone.gemfire.cache.hdfs.internal;

import java.util.concurrent.TimeUnit;

/**
 * Observes and reacts to flush events.
 * 
 * @author bakera
 */
public interface FlushObserver {
  public interface AsyncFlushResult {
    /**
     * Waits for the most recently enqueued batch to completely flush.
     * 
     * @param time the time to wait
     * @param unit the time unit
     * @return true if flushed before the timeout
     * @throws InterruptedException interrupted while waiting
     */
    public boolean waitForFlush(long time, TimeUnit unit) throws InterruptedException;
  }

  /**
   * Returns true when the queued events should be drained from the queue
   * immediately.
   * 
   * @return true if draining
   */
  boolean shouldDrainImmediately();
  
  /**
   * Begins the flushing the queued events.
   * 
   * @return the async result
   */
  public AsyncFlushResult flush();
}

