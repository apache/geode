/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.persistence;

import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CreateRegionProcessor;

/**
 * Similar to CreateRegionProcessor, this class is used during the initialization of a persistent
 * region to exchange profiles with other members. This class also determines which member should be
 * used for initialization.
 *
 */
public class CreatePersistentRegionProcessor extends CreateRegionProcessor {

  private final PersistenceAdvisor persistenceAdvisor;
  private final boolean recoverFromDisk;

  public CreatePersistentRegionProcessor(CacheDistributionAdvisee advisee,
      PersistenceAdvisor persistenceAdvisor, boolean recoverFromDisk) {
    super(advisee);
    this.persistenceAdvisor = persistenceAdvisor;
    this.recoverFromDisk = recoverFromDisk;
  }

  /**
   * Returns the member id of the member who has the latest copy of the persistent region. This may
   * be the local member ID if this member has the latest known copy.
   *
   * This method will block until the latest member is online.
   *
   * @throws ConflictingPersistentDataException if there are active members which are not based on
   *         the state that is persisted in this member.
   */
  @Override
  public CacheDistributionAdvisor.InitialImageAdvice getInitialImageAdvice(
      CacheDistributionAdvisor.InitialImageAdvice previousAdvice) {
    return persistenceAdvisor.getInitialImageAdvice(previousAdvice, recoverFromDisk);
  }
}
