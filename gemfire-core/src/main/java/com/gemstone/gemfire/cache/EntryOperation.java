/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * Gemfire Context passed to <code>PartitionedResolver</code> to compute the
 * data location
 * 
 * @author Yogesh Mahajan
 * @author Mitch Thomas
 * 
 * @see PartitionResolver  
 * @since 6.0 
 */
public interface EntryOperation<K,V> {

  /** 
   * Returns the region to which this cached object belongs or
   * the region that raised this event for <code>RegionEvent</code>s.
   * @return the region associated with this object or the region that raised
   * this event.
   */
  public Region<K,V> getRegion();

  /**
   * Return a description of the operation that triggered this event.
   * It may return null and should not be used to generate routing object
   * in {@link PartitionResolver#getRoutingObject(EntryOperation)}
   * @return the operation that triggered this event.
   * @since 6.0
   * @deprecated
   */
  public Operation getOperation();

  /** 
   * Returns the key.
   * @return the key
   */
  public K getKey();

  /** Returns the callbackArgument passed to the method that generated this event.
   * Provided primarily in case this object or region has already been
   * destroyed. See the {@link Region} interface methods that take a
   * callbackArgument parameter.
   * Only fields on the key should be used when creating the routing object.
   * @return the callbackArgument associated with this event. <code>null</code>
   * is returned if the callback argument is not propagated to the event.
   * This happens for events given to {@link TransactionListener}
   * and to {@link CacheListener} on the remote side of a transaction commit.
   */
  public Object getCallbackArgument();

  /**
   * Returns <code>true</code> if the callback argument is "available".
   * Not available means that the callback argument may have existed but it could
   * not be obtained.
   * Note that {@link #getCallbackArgument} will return <code>null</code>
   * when this method returns <code>false</code>.
   * @since 6.0
   */
  public boolean isCallbackArgumentAvailable();
  
  /**
   * Returns the value but may return null and should not be used to generate 
   * routing object in {@link PartitionResolver#getRoutingObject(EntryOperation)}.
   *  Only fields on the key should be used when creating the routing object.
   * @return the value.
   * @deprecated
   */
  public V getNewValue();
}
