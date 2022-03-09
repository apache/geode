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
 *
 */

package org.apache.geode.management.internal.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.BaseManagementService;
import org.apache.geode.management.internal.functions.RestoreRedundancyFunction;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

public class RestoreRedundancyPerformerTest {

  private static final String DS_MEMBER_NAME_SERVER1 = "server1";
  private static final String DS_MEMBER_NAME_SERVER2 = "server2";

  private static final String REGION_1 = "region1";
  private static final String BOGUS_PASS_MESSAGE = "Bogus pass message";
  private InternalDistributedMember server1;
  private InternalDistributedMember server2;
  private InternalCacheForClientAccess internalCacheForClientAccess;
  private RestoreRedundancyPerformer restoreRedundancyPerformer;

  @Before
  public void setup() {
    BaseManagementService baseManagementService = mock(BaseManagementService.class);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    DistributedRegionMXBean distributedRegionMXBean = mock(DistributedRegionMXBean.class);
    server1 = mock(InternalDistributedMember.class);
    server2 = mock(InternalDistributedMember.class);
    internalCacheForClientAccess = mock(InternalCacheForClientAccess.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(baseManagementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    when(baseManagementService.getDistributedRegionMXBean(anyString()))
        .thenReturn(distributedRegionMXBean);
    when(distributedRegionMXBean.getRegionType()).thenReturn(String.valueOf(DataPolicy.PARTITION));
    when(distributedRegionMXBean.getMembers())
        .thenReturn(new String[] {DS_MEMBER_NAME_SERVER1, DS_MEMBER_NAME_SERVER2});
    when(server1.getName()).thenReturn(DS_MEMBER_NAME_SERVER1);
    when(server2.getName()).thenReturn(DS_MEMBER_NAME_SERVER2);
    when(distributedSystemMXBean.listAllRegionPaths()).thenReturn(new String[] {REGION_1});
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(distributionManager);
    Set<InternalDistributedMember> dsMembers = new HashSet<>();
    dsMembers.add(server1);
    dsMembers.add(server2);
    when(distributionManager.getDistributionManagerIds()).thenReturn(dsMembers);
    BaseManagementService.setManagementService(internalCacheForClientAccess, baseManagementService);

    when(((InternalCache) internalCacheForClientAccess).getCacheForProcessingClientRequests())
        .thenReturn(internalCacheForClientAccess);
    when(internalCacheForClientAccess.getInternalDistributedSystem())
        .thenReturn(internalDistributedSystem);

    when(server1.getVersionOrdinalObject())
        .thenReturn(RestoreRedundancyPerformer.ADDED_VERSION);
    when(server2.getVersionOrdinalObject())
        .thenReturn(RestoreRedundancyPerformer.ADDED_VERSION);

    restoreRedundancyPerformer = new RestoreRedundancyPerformer();
  }

  @Test
  public void executePerformWithIncludeRegionsSuccess() {
    // Setup a request to restore redundancy for region 1
    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();
    restoreRedundancyRequest.setReassignPrimaries(true);
    restoreRedundancyRequest.setIncludeRegions(Collections.singletonList(REGION_1));
    restoreRedundancyRequest.setExcludeRegions(new ArrayList<>());


    // Setup a successful response from executeFunctionAndGetFunctionResult
    RestoreRedundancyResultsImpl restoreRedundancyResultsImpl = new RestoreRedundancyResultsImpl();
    restoreRedundancyResultsImpl.setStatusMessage(BOGUS_PASS_MESSAGE);
    restoreRedundancyResultsImpl.setSuccess(true);

    Map<String, RegionRedundancyStatus> satisfied =
        restoreRedundancyResultsImpl.getSatisfiedRedundancyRegionResults();

    // Create and add the RegionRedundancyStatus to the response
    RegionRedundancyStatusImpl regionRedundancyStatusImpl = new RegionRedundancyStatusImpl(1, 1,
        REGION_1, RegionRedundancyStatus.RedundancyStatus.SATISFIED);

    satisfied.put(REGION_1, regionRedundancyStatusImpl);

    // intercept the executeFunctionAndGetFunctionResult method call on the performer
    RestoreRedundancyPerformer spyRedundancyPerformer = spy(restoreRedundancyPerformer);
    doReturn(restoreRedundancyResultsImpl).when(spyRedundancyPerformer)
        .executeFunctionAndGetFunctionResult(any(RestoreRedundancyFunction.class),
            any(Object.class),
            any(
                DistributedMember.class));

    // invoke perform
    RestoreRedundancyResults restoreRedundancyResult = spyRedundancyPerformer
        .perform(internalCacheForClientAccess, restoreRedundancyRequest, false);

    assertThat(restoreRedundancyResult.getSuccess()).isTrue();
  }

  @Test
  public void executePerformWithNoIncludeRegionsSuccess() {
    // Setup a request to restore redundancy for region 1
    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();
    restoreRedundancyRequest.setReassignPrimaries(true);


    // Setup a successful response from executeFunctionAndGetFunctionResult
    RestoreRedundancyResultsImpl restoreRedundancyResultsImpl = new RestoreRedundancyResultsImpl();
    restoreRedundancyResultsImpl.setStatusMessage(BOGUS_PASS_MESSAGE);
    restoreRedundancyResultsImpl.setSuccess(true);

    Map<String, RegionRedundancyStatus> satisfied =
        restoreRedundancyResultsImpl.getSatisfiedRedundancyRegionResults();

    // Create and add the RegionRedundancyStatus to the response
    RegionRedundancyStatusImpl regionRedundancyStatusImpl = new RegionRedundancyStatusImpl(1, 1,
        REGION_1, RegionRedundancyStatus.RedundancyStatus.SATISFIED);

    satisfied.put(REGION_1, regionRedundancyStatusImpl);

    // intercept the executeFunctionAndGetFunctionResult method call on the performer
    RestoreRedundancyPerformer spyRedundancyPerformer = spy(restoreRedundancyPerformer);
    doReturn(restoreRedundancyResultsImpl).when(spyRedundancyPerformer)
        .executeFunctionAndGetFunctionResult(any(RestoreRedundancyFunction.class),
            any(Object.class),
            any(
                DistributedMember.class));

    // invoke perform
    RestoreRedundancyResults restoreRedundancyResult = spyRedundancyPerformer
        .perform(internalCacheForClientAccess, restoreRedundancyRequest, false);

    assertThat(restoreRedundancyResult.getSuccess()).isTrue();
  }

  @Test
  public void executePerformFailure() {
    // Setup a request to restore redundancy for region 1
    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();
    restoreRedundancyRequest.setReassignPrimaries(true);
    restoreRedundancyRequest.setIncludeRegions(Collections.singletonList(REGION_1));
    restoreRedundancyRequest.setExcludeRegions(new ArrayList<>());


    // Setup a successful response from executeFunctionAndGetFunctionResult
    RestoreRedundancyResultsImpl restoreRedundancyResultsImpl = new RestoreRedundancyResultsImpl();
    restoreRedundancyResultsImpl.setStatusMessage(BOGUS_PASS_MESSAGE);
    restoreRedundancyResultsImpl.setSuccess(false);

    Map<String, RegionRedundancyStatus> underRedundancyRegionResults =
        restoreRedundancyResultsImpl.getUnderRedundancyRegionResults();

    // Create and add the RegionRedundancyStatus to the response
    RegionRedundancyStatusImpl regionRedundancyStatusImpl = new RegionRedundancyStatusImpl(1, 1,
        REGION_1, RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED);

    underRedundancyRegionResults.put(REGION_1, regionRedundancyStatusImpl);


    // intercept the executeFunctionAndGetFunctionResult method call on the performer
    RestoreRedundancyPerformer spyRedundancyPerformer = spy(restoreRedundancyPerformer);
    doReturn(restoreRedundancyResultsImpl).when(spyRedundancyPerformer)
        .executeFunctionAndGetFunctionResult(any(RestoreRedundancyFunction.class),
            any(Object.class),
            any(
                DistributedMember.class));

    // invoke perform
    RestoreRedundancyResults restoreRedundancyResult = spyRedundancyPerformer
        .perform(internalCacheForClientAccess, restoreRedundancyRequest, false);

    assertThat(restoreRedundancyResult.getSuccess()).isFalse();
    assertThat(restoreRedundancyResult.getStatusMessage()).contains(BOGUS_PASS_MESSAGE);
  }

  @Test
  public void executePerformNoViableMembers() {
    // Setup a request to restore redundancy for region 1
    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();
    restoreRedundancyRequest.setReassignPrimaries(true);
    restoreRedundancyRequest.setIncludeRegions(Collections.singletonList(REGION_1));
    restoreRedundancyRequest.setExcludeRegions(new ArrayList<>());


    // Setup a successful response from executeFunctionAndGetFunctionResult
    RestoreRedundancyResultsImpl restoreRedundancyResultsImpl = new RestoreRedundancyResultsImpl();
    restoreRedundancyResultsImpl.setStatusMessage("Bogus pass message");
    restoreRedundancyResultsImpl.setSuccess(false);

    Map<String, RegionRedundancyStatus> underRedundancyRegionResults =
        restoreRedundancyResultsImpl.getUnderRedundancyRegionResults();

    // Create and add the RegionRedundancyStatus to the response
    RegionRedundancyStatusImpl regionRedundancyStatusImpl = new RegionRedundancyStatusImpl(1, 1,
        REGION_1, RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED);

    underRedundancyRegionResults.put(REGION_1, regionRedundancyStatusImpl);


    when(server1.getVersionOrdinalObject())
        .thenReturn(Version.GEODE_1_2_0);
    when(server2.getVersionOrdinalObject())
        .thenReturn(Version.GEODE_1_9_0);


    // intercept the executeFunctionAndGetFunctionResult method call on the performer
    RestoreRedundancyPerformer spyRedundancyPerformer = spy(restoreRedundancyPerformer);
    doReturn(restoreRedundancyResultsImpl).when(spyRedundancyPerformer)
        .executeFunctionAndGetFunctionResult(any(RestoreRedundancyFunction.class),
            any(Object.class),
            any(
                DistributedMember.class));

    // invoke perform
    RestoreRedundancyResults restoreRedundancyResult = spyRedundancyPerformer
        .perform(internalCacheForClientAccess, restoreRedundancyRequest, false);

    assertThat(restoreRedundancyResult.getSuccess()).isFalse();
    assertThat(restoreRedundancyResult.getStatusMessage())
        .contains("No members with a version greater than or equal");
  }
}
