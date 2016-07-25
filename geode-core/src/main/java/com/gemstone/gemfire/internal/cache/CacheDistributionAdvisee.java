/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.CacheProfile;

/**
 * Distributed cache object (typically a <code>Region</code>) which uses
 * a {@link CacheDistributionAdvisor}.
 * @since GemFire 5.1
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
