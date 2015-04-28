/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

/**
 * Provides notification of {@link com.gemstone.gemfire.cache GemFire Cache}
 * resource management events.
 * 
 * @since 6.0
 */
public interface ResourceListener<T> {
  /**
  * Invoked when a {@link ResourceEvent resource event} occurs.
  * Implementation of this method should be light weight.
  * @param event the resource event
  */
  public void onEvent(T event);
}
