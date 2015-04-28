/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.CacheProfile;

/**
 * Distributed cache object (typically a <code>Region</code>) which uses
 * a {@link CacheDistributionAdvisor}.
 * @author Kirk Lund
 * @since 5.1
 */
public interface CacheDistributionAdvisee extends DistributionAdvisee {

  /**
   * Returns the <code>CacheDistributionAdvisor</code> that provides advice for
   * this advisee.
   * @return the <code>CacheDistributionAdvisor</code>
   */
  public CacheDistributionAdvisor getCacheDistributionAdvisor();

  /**
   * Returns the <code>Cache</code> associated with this cache object.
   * @return the Cache
   */
  public Cache getCache();
  
  /** 
   * Returns the <code>RegionAttributes</code> associated with this advisee.
   * @return the <code>RegionAttributes</code> of this advisee
   */
  public RegionAttributes getAttributes();
  
  /**
   * notifies the advisee that a new remote member has registered a profile
   * showing that it is now initialized
   * 
   * @param profile the remote member's profile
   */
  public void remoteRegionInitialized(CacheProfile profile);
}
