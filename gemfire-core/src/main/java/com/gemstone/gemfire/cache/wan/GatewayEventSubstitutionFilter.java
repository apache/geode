/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.wan;

import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.cache.EntryEvent;

/**
 * Interface <code>GatewayEventSubstitutionFilter</code> provides a way to
 * specify a substitute value to be stored in the <code>GatewayQueueEvent</code>
 * and enqueued in the <code>RegionQueue</code>.
 */
public interface GatewayEventSubstitutionFilter<K,V> extends CacheCallback {

  /**
   * Return the substitute value to be stored in the
   * <code>GatewayQueueEvent</code> and enqueued in the <code>RegionQueue</code>
   * 
   * @param event
   *          The originating <code>EntryEvent</code>
   * @return the substitute value to be stored in the
   *         <code>GatewayQueueEvent</code> and enqueued in the
   *         <code>RegionQueue</code>
   */
  public Object getSubstituteValue(EntryEvent<K,V> event);
}
