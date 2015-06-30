package com.gemstone.gemfire.cache.util;

/**
 * 
 * From 9.0 old wan support is removed. Ideally Gateway (used for old wan) should be removed but it it there for 
 * rolling upgrade support when GatewaySenderProfile update request comes from or sent to old member.
 * Old member uses Gateway$OrderPolicy while latest members uses GatewaySender#OrderPolicy
 * 
 * @author kbachhav
 * @since 9.0
 *
 */
public class Gateway {

  /**
   * The order policy. This enum is applicable only when concurrency-level is > 1.
   * 
   * @since 6.5.1
   */
  public enum OrderPolicy {
    /**
     * Indicates that events will be parallelized based on the event's
     * originating member and thread
     */
    THREAD,
    /**
     * Indicates that events will be parallelized based on the event's key
     */
    KEY,
    /** Indicates that events will be parallelized based on the event's:
     *  - partition (using the PartitionResolver) in the case of a partitioned
     *    region event
     *  - key in the case of a replicated region event
     */
    PARTITION
  }
}
