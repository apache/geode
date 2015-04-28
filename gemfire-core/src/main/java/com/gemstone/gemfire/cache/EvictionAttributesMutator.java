package com.gemstone.gemfire.cache;
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;

/**
 * The EvictionAttributesMutator allows changes to be made to a 
 * {@link com.gemstone.gemfire.cache.EvictionAttributes}. It is returned
 * by {@link com.gemstone.gemfire.cache.AttributesMutator#getEvictionAttributesMutator()}
 * @author Mitch Thomas
 * @since 5.0
 */
public interface EvictionAttributesMutator
{
  /**
   * Sets the maximum value on the {@link EvictionAttributesImpl} that the given
   * {@link EvictionAlgorithm} uses to determine when to perform its
   * {@link EvictionAction}. The unit of the maximum value is determined by the
   * {@link EvictionAlgorithm}
   * 
   * @param maximum
   *          value used by the {@link EvictionAlgorithm}
   */
  public void setMaximum(int maximum);
}
