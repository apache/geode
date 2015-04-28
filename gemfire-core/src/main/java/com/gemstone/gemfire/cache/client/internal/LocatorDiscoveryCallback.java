/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

/**
 * A callback to receive notifications about locator discovery. Currently 
 * only used internally.
 * @author dsmith
 * @since 5.7
 */
public interface LocatorDiscoveryCallback {
  
  /**
   * Called to indicate that new locators
   * have been discovered
   * @param locators a list of InetSocketAddresses of new
   * locators that have been discovered.
   */
  void locatorsDiscovered(List locators);
  
  /**
   * Called to indicated that locators
   * have been removed from the list
   * of available locators.
   * @param locators a list of InetSocketAddresses
   * of locators that have been removed
   */
  void locatorsRemoved(List locators);
  

}
