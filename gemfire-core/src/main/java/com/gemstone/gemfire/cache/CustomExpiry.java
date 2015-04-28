/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * This is the contract that a <code>custom-expiry</code> element must honor.
 * It determines the expiration characteristics for a specific entry in a region.
 * <p>Note that if you wish to refer to an implementation of this interface in XML,
 * the implementation must also implement the Declarable interface.
 * 
 * @author jpenney
 *
 */
public interface CustomExpiry<K,V> extends CacheCallback {

  /**
   * Calculate the expiration for a given entry.
   * Returning null indicates that the
   * default for the region should be used.
   * <p>
   * The entry parameter should not be used after this method invocation completes.
   * @param entry the entry to calculate the expiration for
   * @return the expiration to be used, null if the region's defaults should be
   * used.
   */
  public ExpirationAttributes getExpiry(Region.Entry<K,V> entry);
}
