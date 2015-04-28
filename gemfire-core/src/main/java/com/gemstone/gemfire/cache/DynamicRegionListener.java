/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * <code>DynamicRegionListener</code> is an interface that can be
 * implemented to handle dynamic region-related events.

 * The methods on a <code>DynamicRegionListener</code> are invoked synchronously.
 * If the listener method takes a long time to execute then it will cause the
 * operation that caused it to be invoked to take a long time.
 * <p>
 * Note: It is possible to receive duplicate create events when the DynamicRegionFactory
 * goes active due to Cache creation.
 * <p>
 * See {@link DynamicRegionFactory}
 *
 * @author Gideon Low
 * @since 4.3
 */
public interface DynamicRegionListener {

  /**
   * Handles the 'before region creation' event of a dynamic region. This method
   * is invoked before the dynamic region is created in the local VM.
   *
   * @param parentRegionName The name of the parent region
   * @param regionName The name of the region being created
   */
  public void beforeRegionCreate(String parentRegionName, String regionName);

  /**
   * Handles the 'after region creation' event of a dynamic region. This method
   * is invoked after the dynamic region is created in the local VM.
   *
   * @param event A <code>RegionEvent</code> describing the event
   */
  public void afterRegionCreate(RegionEvent<?,?> event);

  /**
   * Handles the 'before region destroyed' event of a dynamic region. This method
   * is invoked before the dynamic region is destroyed in the local VM.
   *
   * @param event A <code>RegionEvent</code> describing the event
   */
  public void beforeRegionDestroy(RegionEvent<?,?> event);

  /**
   * Handles the 'after region destroyed' event of a dynamic region. This method
   * is invoked after the dynamic region is destroyed in the local VM.
   *
   * @param event A <code>RegionEvent</code> describing the event
   */
  public void afterRegionDestroy(RegionEvent<?,?> event);
}
