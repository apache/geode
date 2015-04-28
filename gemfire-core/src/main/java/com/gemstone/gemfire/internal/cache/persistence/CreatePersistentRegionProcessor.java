/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.cache.persistence.ConflictingPersistentDataException;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisee;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor;
import com.gemstone.gemfire.internal.cache.CreateRegionProcessor;

/**
 * Similar to CreateRegionProcessor, this class is used during
 * the initialization of a persistent region to exchange
 * profiles with other members. This class also determines which
 * member should be used for initialization.
 * @author dsmith
 *
 */
public class CreatePersistentRegionProcessor extends CreateRegionProcessor {

  private final PersistenceAdvisor persistenceAdvisor;
  private final boolean recoverFromDisk;

  public CreatePersistentRegionProcessor(CacheDistributionAdvisee advisee,
      PersistenceAdvisor persistenceAdvisor, boolean recoverFromDisk) {
    super(advisee);
    this.persistenceAdvisor= persistenceAdvisor;
    this.recoverFromDisk = recoverFromDisk;
  }
  
  /**
   * Returns the member id of the member who has the latest
   * copy of the persistent region. This may be the local member ID
   * if this member has the latest known copy.
   * 
   * This method will block until the latest member is online.
   * @throws ConflictingPersistentDataException if there are active members
   * which are not based on the state that is persisted in this member.
   */
  @Override
  public CacheDistributionAdvisor.InitialImageAdvice getInitialImageAdvice(
      CacheDistributionAdvisor.InitialImageAdvice previousAdvice) {
    return this.persistenceAdvisor.getInitialImageAdvice(previousAdvice, recoverFromDisk);
  }
}
