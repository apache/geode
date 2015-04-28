/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.cache.RegionAttributes;

/**
 * A snapshot of a GemFire <code>Region</code>
 */
public interface RegionSnapshot extends CacheSnapshot {

  /**
   * Returns the attributes of the <code>Region</code>
   */
  public RegionAttributes getAttributes();
}
