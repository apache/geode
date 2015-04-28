/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
/**
 * Composite data type used to distribute the eviction attributes for
 * a {@link Region}.
 * 
 * @author rishim
 * @since 7.0
 */

public class EvictionAttributesData {
  
  /**
   * Algorithm used for eviction
   */
  private String algorithm;

  /**
   * Maximum entries in Region before eviction starts
   */
  private Integer maximum;

  /**
   * Action to be taken if entries reaches maximum value
   */
  private String action;
  
  @ConstructorProperties( { "algorithm", "maximum", "action"

  })
  public EvictionAttributesData(String algorithm, Integer maximum, String action){
    this.algorithm = algorithm;
    this.maximum = maximum;
    this.action = action;
  }

  /**
   * Returns the algorithm (policy) used to determine which entries will be
   * evicted.
   */
  public String getAlgorithm() {
    return algorithm;
  }

  /**
   * The unit of this value is determined by the definition of the {@link EvictionAlgorithm} set by one of the creation
   * methods e.g. {@link EvictionAttributes#createLRUEntryAttributes()}
   *
   * For algorithm LRU HEAP null will be returned
   * @return maximum value used by the {@link EvictionAlgorithm} which determines when the {@link EvictionAction} is
   *         performed.
   */
  public Integer getMaximum() {
    return maximum;
  }

  /**
   * Returns the action that will be taken on entries that are evicted.
   */
  public String getAction() {
    return action;
  }

  @Override
  public String toString() {

    final StringBuilder buffer = new StringBuilder(128);
    buffer.append("EvictionAttributesData [");

    buffer.append("algorithm=").append(this.getAlgorithm());
    if (!this.getAlgorithm().equals(EvictionAlgorithm.NONE.toString())) {
      buffer.append(", action=").append(this.getAction());
      if (!this.getAlgorithm().equals(EvictionAlgorithm.LRU_HEAP.toString())) {
        buffer.append(", maximum=").append(this.getMaximum());
      }
    }
    buffer.append("]");
    return buffer.toString();
  }


}
