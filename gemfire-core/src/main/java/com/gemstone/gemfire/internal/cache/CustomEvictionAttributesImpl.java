/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. GoPivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CustomEvictionAttributes;
import com.gemstone.gemfire.cache.EvictionCriteria;

/**
 * Concrete instance of {@link CustomEvictionAttributes}.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public final class CustomEvictionAttributesImpl extends
    CustomEvictionAttributes {

  public CustomEvictionAttributesImpl(EvictionCriteria<?, ?> criteria,
      long startTime, long interval, boolean evictIncoming) {
    super(criteria, startTime, interval, evictIncoming);
  }
}
