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
package org.apache.geode.internal.cache.ha;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * A static class which is created only for for testing purposes as some existing tests extend the
 * HARegionQueue. Since the constructors of HARegionQueue are private , this class can act as a
 * bridge between the user defined HARegionQueue class & the actual class. This class object will
 * be buggy as it will tend to publish the Object o QRM thread & the expiry thread before the
 * complete creation of the HARegionQueue instance
 */
class TestOnlyHARegionQueue extends HARegionQueue {

  TestOnlyHARegionQueue(String regionName, InternalCache cache, StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    this(regionName, cache, HARegionQueueAttributes.DEFAULT_HARQ_ATTRIBUTES, new HashMap(),
        Handshake.CONFLATION_DEFAULT, false, statisticsClock);
  }

  TestOnlyHARegionQueue(String regionName, InternalCache cache, HARegionQueueAttributes hrqa,
      Map haContainer, byte clientConflation, boolean isPrimary, StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    super(regionName, cache, haContainer, null, clientConflation, isPrimary, statisticsClock);
    ExpirationAttributes expirationAttributes =
        new ExpirationAttributes(hrqa.getExpiryTime(), ExpirationAction.LOCAL_INVALIDATE);
    region.setOwner(this);
    region.getAttributesMutator().setEntryTimeToLive(expirationAttributes);
    initialized.set(true);
  }

  /**
   * Overloaded constructor to pass an {@code HashMap} instance as a haContainer.
   *
   * @since GemFire 5.7
   */
  TestOnlyHARegionQueue(String regionName, InternalCache cache, HARegionQueueAttributes hrqa,
      StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    this(regionName, cache, hrqa, new HashMap(), Handshake.CONFLATION_DEFAULT, false,
        statisticsClock);
  }
}
