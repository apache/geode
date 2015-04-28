/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.HashSet;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;

/**
 * Used to exchange profiles during region initialization and determine the
 * targets for GII. There are currently two implementations, one for persistent
 * regions and one of non persistent regions. The persistent region
 * implementation will wait for members to come online that may have a later
 * copies of the region.
 * 
 * @author dsmith
 * 
 */
public interface ProfileExchangeProcessor {
  /** Exchange profiles with other members to initialize the region*/
  void initializeRegion();
  /** Get, and possibling wait for, the members that we should initialize from. */ 
  InitialImageAdvice getInitialImageAdvice(InitialImageAdvice previousAdvice);
  
  void setOnline(InternalDistributedMember target);
}
