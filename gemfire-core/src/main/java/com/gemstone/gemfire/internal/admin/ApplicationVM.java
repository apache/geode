/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

//import com.gemstone.gemfire.cache.Region;
//import java.util.Set;

/**
 * Represents one application vm (as opposed to a GemFire system
 * manager) connected to a GemFire distributed system
 */
public interface ApplicationVM extends GemFireVM {

  /**
   * Returns whether or not this "application" VM is a dedicated cache
   * server.
   *
   * @see com.gemstone.gemfire.internal.cache.CacheServerLauncher
   */
  public boolean isDedicatedCacheServer();
}
