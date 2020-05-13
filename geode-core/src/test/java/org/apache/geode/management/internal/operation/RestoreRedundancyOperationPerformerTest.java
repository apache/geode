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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RestoreRedundancyResponse;

public class RestoreRedundancyOperationPerformerTest {

  @Test
  public void executeRestoreRedundancyOnDSWithNoRegionsReturnsSuccessAndNoRegionMessage() {
    ManagementService managementService = mock(ManagementService.class);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listRegions()).thenReturn(new String[] {});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
    RestoreRedundancyOperationPerformer.FunctionExecutor
        functionExecutor =
        mock(RestoreRedundancyOperationPerformer.FunctionExecutor.class);
    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();
    restoreRedundancyRequest.setReassignPrimaries(true);
    RestoreRedundancyResponse
        restoreRedundancyResponse =
        RestoreRedundancyOperationPerformer
            .executeRestoreRedundancyOnDS(managementService, cache, restoreRedundancyRequest,
                functionExecutor);
    assertThat(restoreRedundancyResponse.getSuccess()).isTrue();
    assertThat(restoreRedundancyResponse.getStatusMessage()).isEqualTo(
        CliStrings.REDUNDANCY__MSG__NO_RESTORE_REDUNDANCY_REGIONS_ON_DS);
  }
}
