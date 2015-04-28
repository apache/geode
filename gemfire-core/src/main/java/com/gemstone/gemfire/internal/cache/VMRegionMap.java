/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

/**
 * Internal implementation of {@link RegionMap} for regions stored
 * in normal VM memory.
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
final class VMRegionMap extends AbstractRegionMap {

  VMRegionMap(Object owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
    initialize(owner, attr, internalRegionArgs, false/*isLRU*/);
  }



}
