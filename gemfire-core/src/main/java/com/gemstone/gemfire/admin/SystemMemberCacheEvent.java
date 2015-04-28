package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.cache.Operation;
/**
 * An event that describes an operation on a cache.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a cache is created or closed.
 *
 * @author Darrel Schneider
 * @since 5.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface SystemMemberCacheEvent extends SystemMembershipEvent {
  /**
   * Returns the actual operation that caused this event.
   */
  public Operation getOperation();
}
