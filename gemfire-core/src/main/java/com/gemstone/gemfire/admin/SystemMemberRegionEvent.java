package com.gemstone.gemfire.admin;

/**
 * An event that describes an operation on a region.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a region comes or goes.
 *
 * @author Darrel Schneider
 * @since 5.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface SystemMemberRegionEvent extends SystemMemberCacheEvent {
  /**
   * Returns the full path of the region the event was done on.
   */
  public String getRegionPath();
}
