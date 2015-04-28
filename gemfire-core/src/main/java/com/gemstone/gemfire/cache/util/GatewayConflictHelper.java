/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

/**
 * GatewayConflictHelper is used by an GatewayConflictResolver to decide what to
 * do with an event received from another distributed system that is going to overwrite
 * the current cache state.
 * @since 7.0
 * @author Bruce Schuchardt
 */
public interface GatewayConflictHelper {
  /** disallow the event */
  public void disallowEvent();

  /** modify the value stored in the cache */
  public void changeEventValue(Object value);
}


