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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;

public class SenderIdMonitorTest {
  private final InternalRegion region = mock(InternalRegion.class);
  private final CacheDistributionAdvisor advisor = createCacheDistributionAdvisor(region);
  private final SenderIdMonitor senderIdMonitor =
      SenderIdMonitor.createSenderIdMonitor(region, advisor);

  @Test
  public void checkPassesByDefault() {
    senderIdMonitor.checkSenderIds();
  }

  @Test
  public void checkPassesOnRegionWithNoIdsAndAdvisorWithNoProfiles() {
    senderIdMonitor.update();
    senderIdMonitor.checkSenderIds();
  }

  @Test
  public void checkPassesOnRegionWithGatewayIdsAndAdvisorWithNoProfiles() {
    when(region.getGatewaySenderIds()).thenReturn(Collections.singleton("gatewayId"));
    senderIdMonitor.update();
    senderIdMonitor.checkSenderIds();
  }

  @Test
  public void checkPassesOnRegionWithAsyncEventQueueIdsAndAdvisorWithNoProfiles() {
    when(region.getVisibleAsyncEventQueueIds())
        .thenReturn(Collections.singleton("AsyncEventQueueId"));
    senderIdMonitor.update();
    senderIdMonitor.checkSenderIds();
  }

  @Test
  public void checkLogsWarningWithRegionAndProfileWithDifferentGatewayIds() {
    when(region.getName()).thenReturn("regionName");
    when(region.getGatewaySenderIds()).thenReturn(Collections.singleton("gatewayId"));
    CacheProfile profile = new CacheProfile();
    profile.gatewaySenderIds = Collections.singleton("profileGatewayId");
    advisor.putProfile(profile, true);

    senderIdMonitor.checkSenderIds();

    assertThat(senderIdMonitor.getGatewaySenderIdsDifferWarningMessage()).isTrue();
    assertThat(senderIdMonitor.getAsyncQueueIdsDifferWarningMessage()).isFalse();

    senderIdMonitor.checkSenderIds(); // should not log duplicate message

    when(region.getGatewaySenderIds()).thenReturn(Collections.singleton("profileGatewayId"));
    senderIdMonitor.update();

    senderIdMonitor.checkSenderIds(); // should log all is well message

    assertThat(senderIdMonitor.getGatewaySenderIdsDifferWarningMessage()).isFalse();
    assertThat(senderIdMonitor.getAsyncQueueIdsDifferWarningMessage()).isFalse();
  }

  @Test
  public void checkPassesWithRegionAndProfileWithDifferentGatewayIdsThatBecomeEqual() {
    when(region.getGatewaySenderIds()).thenReturn(Collections.singleton("gatewayId"));
    CacheProfile profile = new CacheProfile();
    profile.gatewaySenderIds = Collections.singleton("profileGatewayId");
    advisor.putProfile(profile, true);
    when(region.getGatewaySenderIds()).thenReturn(Collections.singleton("profileGatewayId"));
    senderIdMonitor.update();
    senderIdMonitor.checkSenderIds();
  }

  @Test
  public void checkLogsWarningWithRegionAndProfileWithDifferentAsyncEventQueueIds() {
    when(region.getName()).thenReturn("regionName");
    when(region.getVisibleAsyncEventQueueIds())
        .thenReturn(Collections.singleton("AsyncEventQueueId"));
    CacheProfile profile = new CacheProfile();
    profile.asyncEventQueueIds = Collections.singleton("profileAsyncEventQueueId");
    advisor.putProfile(profile, true);

    senderIdMonitor.checkSenderIds();

    assertThat(senderIdMonitor.getAsyncQueueIdsDifferWarningMessage()).isTrue();
    assertThat(senderIdMonitor.getGatewaySenderIdsDifferWarningMessage()).isFalse();

    senderIdMonitor.checkSenderIds(); // should not log duplicate message

    when(region.getVisibleAsyncEventQueueIds())
        .thenReturn(Collections.singleton("profileAsyncEventQueueId"));
    senderIdMonitor.update();

    senderIdMonitor.checkSenderIds(); // should log all is well message

    assertThat(senderIdMonitor.getAsyncQueueIdsDifferWarningMessage()).isFalse();
    assertThat(senderIdMonitor.getGatewaySenderIdsDifferWarningMessage()).isFalse();
  }

  @Test
  public void checkPassesWithRegionAndProfileWithDifferentAsyncEventQueueIdsThatBecomeEqual() {
    when(region.getVisibleAsyncEventQueueIds())
        .thenReturn(Collections.singleton("AsyncEventQueueId"));
    CacheProfile profile = new CacheProfile();
    profile.asyncEventQueueIds = Collections.singleton("profileAsyncEventQueueId");
    advisor.putProfile(profile, true);
    when(region.getVisibleAsyncEventQueueIds())
        .thenReturn(Collections.singleton("profileAsyncEventQueueId"));
    senderIdMonitor.update();
    senderIdMonitor.checkSenderIds();
  }

  private CacheDistributionAdvisor createCacheDistributionAdvisor(InternalRegion region) {
    DistributionAdvisor advisor = mock(DistributionAdvisor.class);
    CacheDistributionAdvisee advisee = mock(CacheDistributionAdvisee.class);
    when(advisee.getDistributionAdvisor()).thenReturn(advisor);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(advisee.getDistributionManager()).thenReturn(distributionManager);
    CacheDistributionAdvisor result =
        CacheDistributionAdvisor.createCacheDistributionAdvisor(advisee);
    result.initializationGate();
    return result;
  }

}
