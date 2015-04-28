/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Contains information about an event affecting a region, including
 * its identity and the circumstances of the event.
 * This is passed in to <code>CacheListener</code> and <code>CacheWriter</code>.
 *
 * @author Eric Zoerner
 *
 *
 * @see CacheListener
 * @see CacheWriter
 * @see EntryEvent
 * @since 2.0
 */
public interface RegionEvent<K,V> extends CacheEvent<K,V> {
  
  /**
   * Return true if this region was destroyed but is being reinitialized,
   * for example if a snapshot was just loaded. Can only return true for
   * an event related to region destruction.
   */
  public boolean isReinitializing();
  
}
