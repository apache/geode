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
package org.apache.geode.internal.cache;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionAdvisor.InitializationListener;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.ProfileListener;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class SenderIdMonitor implements ProfileListener, InitializationListener {
  private static final Logger logger = LogService.getLogger();
  private final InternalRegion region;
  private final CacheDistributionAdvisor advisor;
  private volatile Set<String> illegalGatewaySenderIds = null;
  private volatile Set<String> illegalAsyncEventQueueIds = null;
  private boolean gatewaySenderIdsDifferWarningMessage;
  private boolean asyncQueueIdsDifferWarningMessage;

  private SenderIdMonitor(InternalRegion region, CacheDistributionAdvisor advisor) {
    this.region = region;
    this.advisor = advisor;
  }

  public static SenderIdMonitor createSenderIdMonitor(InternalRegion region,
      CacheDistributionAdvisor advisor) {
    SenderIdMonitor senderIdMonitor = new SenderIdMonitor(region, advisor);
    advisor.addProfileChangeListener(senderIdMonitor);
    advisor.setInitializationListener(senderIdMonitor);
    return senderIdMonitor;
  }

  @Override
  public void profileCreated(Profile profile) {
    update();
  }

  @Override
  public void profileUpdated(Profile profile) {
    update();
  }

  @Override
  public void profileRemoved(Profile profile, boolean destroyed) {
    update();
  }

  @Override
  public void initialized() {
    update();
  }

  /**
   * Needs to be called if this region's gateways or asyncEventIds change.
   */
  public void update() {
    if (!this.advisor.pollIsInitialized()) {
      return;
    }
    final Set<String> gatewaySenderIds = region.getGatewaySenderIds();
    final Set<String> visibleAsyncEventQueueIds = region.getVisibleAsyncEventQueueIds();
    final AtomicBoolean foundIllegalState = new AtomicBoolean();
    this.advisor.accept((advisor, profile, idx, count, aggregate) -> {
      if (profile instanceof CacheProfile) {
        final CacheProfile cp = (CacheProfile) profile;
        if (!gatewaySenderIds.equals(cp.gatewaySenderIds)) {
          foundIllegalState.set(true);
          illegalGatewaySenderIds = cp.gatewaySenderIds;
        }
        if (!visibleAsyncEventQueueIds.equals(cp.asyncEventQueueIds)) {
          foundIllegalState.set(true);
          illegalAsyncEventQueueIds = cp.asyncEventQueueIds;
        }
      }
      return true;
    }, null);
    if (!foundIllegalState.get()) {
      illegalGatewaySenderIds = null;
      illegalAsyncEventQueueIds = null;
    }
  }

  @VisibleForTesting
  public boolean getGatewaySenderIdsDifferWarningMessage() {
    return gatewaySenderIdsDifferWarningMessage;
  }

  @VisibleForTesting
  public boolean getAsyncQueueIdsDifferWarningMessage() {
    return asyncQueueIdsDifferWarningMessage;
  }


  public void checkSenderIds() {
    if (illegalGatewaySenderIds != null) {
      if (!gatewaySenderIdsDifferWarningMessage) {
        gatewaySenderIdsDifferWarningMessage = true;
        logger.warn(
            "Region {} has {} gateway sender IDs. Another member has the same region with {} gateway sender IDs. For the same region, across all members, gateway sender ids should be same.",
            region.getName(), region.getGatewaySenderIds(), illegalGatewaySenderIds);
      }
    } else {
      if (gatewaySenderIdsDifferWarningMessage) {
        gatewaySenderIdsDifferWarningMessage = false;
        logger.warn(
            "Region {} now has the same gateway sender IDs on all members. The previous problem with them being different has been corrected.",
            region.getName());
      }
    }
    if (illegalAsyncEventQueueIds != null) {
      if (!asyncQueueIdsDifferWarningMessage) {
        asyncQueueIdsDifferWarningMessage = true;
        logger.warn(
            "Region {} has {} AsyncEvent queue IDs. Another member has the same region with {} AsyncEvent queue IDs. For the same region, across all members, AsyncEvent queue IDs should be same.",
            region.getName(), region.getVisibleAsyncEventQueueIds(), illegalAsyncEventQueueIds);
      }
    } else {
      if (asyncQueueIdsDifferWarningMessage) {
        asyncQueueIdsDifferWarningMessage = false;
        logger.warn(
            "Region {} now has the same AsyncEvent queue IDs on all members. The previous problem with them being different has been corrected.",
            region.getName());
      }
    }
  }
}
