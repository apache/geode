/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * User-defined objects that can be plugged into caching to receive callback
 * notifications.
 *
 * @author Eric Zoerner
 *
 * @since 3.0
 */
public interface CacheCallback {
  /** Called when the region containing this callback is closed or destroyed, when
   * the cache is closed, or when a callback is removed from a region
   * using an <code>AttributesMutator</code>.
   *
   * <p>Implementations should cleanup any external
   * resources such as database connections. Any runtime exceptions this method
   * throws will be logged.
   *
   * <p>It is possible for this method to be called multiple times on a single
   * callback instance, so implementations must be tolerant of this.
   *
   * @see Cache#close()
   * @see Region#close
   * @see Region#localDestroyRegion()
   * @see Region#destroyRegion()
   * @see AttributesMutator
   */
  public void close();
}
