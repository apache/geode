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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class ClusterDistributionManagerTest {

  @Test
  public void shouldBeMockable() throws Exception {
    ClusterDistributionManager mockDistributionManager = mock(ClusterDistributionManager.class);
    InternalDistributedMember mockInternalDistributedMember = mock(InternalDistributedMember.class);
    Executor mockExecutor = mock(Executor.class);

    when(mockDistributionManager.getExecutor(anyInt(), eq(mockInternalDistributedMember)))
        .thenReturn(mockExecutor);

    assertThat(mockDistributionManager.getExecutor(1, mockInternalDistributedMember))
        .isSameAs(mockExecutor);
  }
}
