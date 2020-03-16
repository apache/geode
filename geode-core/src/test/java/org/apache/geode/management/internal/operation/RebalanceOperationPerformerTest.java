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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.BaseManagementService;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;

public class RebalanceOperationPerformerTest {

  @Test
  public void executeRebalanceOnDSWithNoRegionsReturnsSuccessAndNoRegionMessage() {
    ManagementService managementService = mock(ManagementService.class);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listRegions()).thenReturn(new String[] {});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
    RebalanceOperationPerformer.FunctionExecutor functionExecutor =
        mock(RebalanceOperationPerformer.FunctionExecutor.class);

    RebalanceResult result = RebalanceOperationPerformer.executeRebalanceOnDS(managementService,
        cache, "true", null, functionExecutor);

    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Distributed system has no regions that can be rebalanced.");
  }

  @Test
  public void executeRebalanceOnDSWithOneRegionReturnsSuccessAndNoRegionMessage() {
    ManagementService managementService = mock(ManagementService.class);
    DistributedRegionMXBean regionMXBean = mock(DistributedRegionMXBean.class);
    when(regionMXBean.getRegionType()).thenReturn("PARTITION");
    when(regionMXBean.getMembers()).thenReturn(new String[] {"member1"});
    when(managementService.getDistributedRegionMXBean("/region1")).thenReturn(regionMXBean);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listRegions()).thenReturn(new String[] {"region1"});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    DistributionManager distributionManager = mock(DistributionManager.class);
    InternalDistributedMember distributedMember = mock(InternalDistributedMember.class);
    when(distributedMember.getUniqueId()).thenReturn("member1");
    Set<InternalDistributedMember> members = Collections.singleton(distributedMember);
    when(distributionManager.getDistributionManagerIds()).thenReturn(members);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    RebalanceOperationPerformer.FunctionExecutor functionExecutor =
        mock(RebalanceOperationPerformer.FunctionExecutor.class);

    RebalanceResult result =
        RebalanceOperationPerformer.executeRebalanceOnDS(managementService, cache, "true",
            Collections.emptyList(), functionExecutor);

    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Distributed system has no regions that can be rebalanced.");
  }

  @Test
  public void executeRebalanceOnDSWithOneRegionOnTwoMembersReturnsSuccessAndRebalanceRegionResult() {
    ManagementService managementService = mock(ManagementService.class);
    DistributedRegionMXBean regionMXBean = mock(DistributedRegionMXBean.class);
    when(regionMXBean.getRegionType()).thenReturn("PARTITION");
    when(regionMXBean.getMembers()).thenReturn(new String[] {"member1", "member2"});
    when(managementService.getDistributedRegionMXBean("/region1")).thenReturn(regionMXBean);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listRegions()).thenReturn(new String[] {"region1"});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    InternalDistributedMember distributedMember1 = mock(InternalDistributedMember.class);
    when(distributedMember1.getUniqueId()).thenReturn("member1");
    InternalDistributedMember distributedMember2 = mock(InternalDistributedMember.class);
    when(distributedMember2.getUniqueId()).thenReturn("member2");
    Set<InternalDistributedMember> members = new HashSet<>();
    members.add(distributedMember1);
    members.add(distributedMember2);
    when(distributionManager.getDistributionManagerIds()).thenReturn(members);
    when(distributionManager.getNormalDistributionManagerIds()).thenReturn(members);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    RebalanceOperationPerformer.FunctionExecutor functionExecutor =
        mock(RebalanceOperationPerformer.FunctionExecutor.class);
    List<Object> resultList = new ArrayList<>();
    resultList.add("0,1,2,3,4,5,6,7,8,/region1");
    when(functionExecutor.execute(any(), any(), any())).thenReturn(resultList);

    RebalanceResult result =
        RebalanceOperationPerformer.executeRebalanceOnDS(managementService, cache, "true",
            Collections.emptyList(), functionExecutor);

    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getRebalanceRegionResults()).isNotNull();
    assertThat(result.getRebalanceRegionResults()).hasSize(1);
    RebalanceRegionResult regionResult = result.getRebalanceRegionResults().get(0);
    assertThat(regionResult.getRegionName()).isEqualTo("region1");
    assertThat(regionResult.getBucketCreateBytes()).isEqualTo(0);
    assertThat(regionResult.getBucketCreateTimeInMilliseconds()).isEqualTo(1);
    assertThat(regionResult.getBucketCreatesCompleted()).isEqualTo(2);
    assertThat(regionResult.getBucketTransferBytes()).isEqualTo(3);
    assertThat(regionResult.getBucketTransferTimeInMilliseconds()).isEqualTo(4);
    assertThat(regionResult.getBucketTransfersCompleted()).isEqualTo(5);
    assertThat(regionResult.getPrimaryTransferTimeInMilliseconds()).isEqualTo(6);
    assertThat(regionResult.getPrimaryTransfersCompleted()).isEqualTo(7);
    assertThat(regionResult.getTimeInMilliseconds()).isEqualTo(8);
  }


  @Test
  public void performWithIncludeRegionsWhenRegionOnNoMembersReturnsFalseWithCorrectMessage() {
    RebalanceOperation rebalanceOperation = mock(RebalanceOperation.class);
    when(rebalanceOperation.getIncludeRegions()).thenReturn(Collections.singletonList("region1"));
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    BaseManagementService managementService = mock(BaseManagementService.class);
    BaseManagementService.setManagementService(cache, managementService);

    RebalanceResult result = RebalanceOperationPerformer.perform(cache, rebalanceOperation);

    assertThat(result.getSuccess()).isFalse();
    assertThat(result.getStatusMessage())
        .isEqualTo("For the region /region1, no member was found.");
  }

}
