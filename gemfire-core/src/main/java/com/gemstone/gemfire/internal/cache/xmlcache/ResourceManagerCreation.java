/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * File comment
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.Set;

import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

/**
 * @author Mitch Thomas
 * @since 6.0
 */
public class ResourceManagerCreation implements ResourceManager {

  private volatile float criticalHeapPercentage;
  private boolean criticalHeapSet = false;

  private volatile float evictionHeapPercentage;
  private boolean evictionHeapSet = false;

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#createRebalanceFactory()
   */
  public RebalanceFactory createRebalanceFactory() {
    throw new IllegalArgumentException("Unused");
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#getRebalanceOperations()
   */
  public Set<RebalanceOperation> getRebalanceOperations() {
    throw new IllegalArgumentException("Unused");
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#getCriticalHeapPercentage()
   */
  public float getCriticalHeapPercentage() {
    return this.criticalHeapPercentage;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#setCriticalHeapPercentage(int)
   */
  public void setCriticalHeapPercentage(float heapPercentage) {
    this.criticalHeapSet = true;
    this.criticalHeapPercentage = heapPercentage;
  }
  
  public void setCriticalHeapPercentageToDefault() {
    this.criticalHeapPercentage = ResourceManager.DEFAULT_CRITICAL_HEAP_PERCENTAGE;
  }

  public void setEvictionHeapPercentageToDefault() {
    this.evictionHeapPercentage = ResourceManager.DEFAULT_EVICTION_HEAP_PERCENTAGE;
  }

  /**
   * Determine if the critical heap was configured
   * @return true if it was configured
   */
  public boolean hasCriticalHeap() {
    return this.criticalHeapSet;
  }

  public void configure(ResourceManager r) {
    if (hasCriticalHeap()) {
      r.setCriticalHeapPercentage(this.criticalHeapPercentage);
    }
    if (hasEvictionHeap()) {
      r.setEvictionHeapPercentage(this.evictionHeapPercentage);
    }
  }

  /**
   * @param other the other ResourceManager with which to compare 
   */
  public void sameAs(ResourceManager other) {
    if (getCriticalHeapPercentage() != other.getCriticalHeapPercentage()) {
      throw new RuntimeException("Resource Manager critical heap percentages differ: "
          + getCriticalHeapPercentage() + " != " + other.getCriticalHeapPercentage());
    }
    if (hasEvictionHeap()) {
      // If we don't have it set don't compare since other may have been set to
      // a smart default.
      if (getEvictionHeapPercentage() != other.getEvictionHeapPercentage()) {
        throw new RuntimeException("Resource Manager eviction heap percentages differ: "
                                   + getEvictionHeapPercentage() + " != " + other.getEvictionHeapPercentage());
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#getEvictionHeapPercentage()
   */
  public float getEvictionHeapPercentage() {
    return this.evictionHeapPercentage;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#setEvictionHeapPercentage(int)
   */
  public void setEvictionHeapPercentage(float heapPercentage) {
    this.evictionHeapSet = true;
    this.evictionHeapPercentage = heapPercentage;
  }

  /**
   * Determine if the eviction heap was configured
   * @return true if the eviction heap was configured
   */
  public boolean hasEvictionHeap() {
    return this.evictionHeapSet;
  }
}
