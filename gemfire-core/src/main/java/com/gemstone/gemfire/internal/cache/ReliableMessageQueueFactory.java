/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Represents a factory for instances of {@link ReliableMessageQueue}.
 * The Cache will have an instance of the factory that can be obtained
 * from {@link GemFireCacheImpl#getReliableMessageQueueFactory}.
 * 
 * @author Darrel Schneider
 * @since 5.0
 */
public interface ReliableMessageQueueFactory {
  /**
   * Creates an instance of {@link ReliableMessageQueue} given the region
   * that the queue will be on.
   * @param region the distributed region that the created queue will service.
   * @return the created queue
   */
  public ReliableMessageQueue create(DistributedRegion region);
  /**
   * Cleanly shutdown this factory flushing any persistent data to disk.
   * @param force true if close should always work
   * @throws IllegalStateException if <code>force</code> is false and the factory
   * is still in use. The factory is in use as long as a queue it produced remains
   * unclosed.
   */
  public void close(boolean force);
}
