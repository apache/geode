/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.common;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;

import javax.servlet.http.HttpSession;

/**
 * Interface to basic cache operations.
 */
public interface SessionCache {

  /**
   * Initialize the cache and create the appropriate region.
   */
  public void initialize();

  /**
   * Stop the cache.
   */
  public void stop();

  /**
   * Retrieve the cache reference.
   *
   * @return a {@code GemFireCache} reference
   */
  public GemFireCache getCache();

  /**
   * Get the {@code Region} being used by client code to put attributes.
   *
   * @return a {@code Region<String, HttpSession>} reference
   */
  public Region<String, HttpSession> getOperatingRegion();

  /**
   * Get the backing {@code Region} being used. This may not be the same as the
   * region being used by client code to put attributes.
   *
   * @return a {@code Region<String, HttpSession>} reference
   */
  public Region<String, HttpSession> getSessionRegion();

  /**
   * Is this cache client-server? The only other alternative is peer-to-peer.
   *
   * @return true if this cache is client-server.
   */
  public boolean isClientServer();
}
