/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import com.gemstone.gemfire.cache.Region;

/**
 * @author dsmith
 *
 */
public interface RegionFilter {
  boolean include(Region<?, ?> region);
}
