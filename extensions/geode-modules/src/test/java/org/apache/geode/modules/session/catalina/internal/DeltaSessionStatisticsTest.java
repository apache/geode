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

package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class DeltaSessionStatisticsTest {

  @Test
  public void CreatedDeltaSessionStatisticsAccessProperStats() {
    final String appName = "DeltaSessionStatisticsTest";

    final InternalDistributedSystem internalDistributedSystem =
        mock(InternalDistributedSystem.class);
    final Statistics statistics = mock(Statistics.class);

    when(internalDistributedSystem.createAtomicStatistics(any(), any())).thenReturn(statistics);

    final DeltaSessionStatistics deltaSessionStatistics =
        new DeltaSessionStatistics(internalDistributedSystem, appName);

    deltaSessionStatistics.incSessionsCreated();
    deltaSessionStatistics.incSessionsExpired();
    deltaSessionStatistics.incSessionsInvalidated();

    deltaSessionStatistics.getSessionsCreated();
    deltaSessionStatistics.getSessionsExpired();
    deltaSessionStatistics.getSessionsInvalidated();

    verify(statistics, times(3)).incLong(anyInt(), anyLong());
    verify(statistics, times(3)).getLong(anyInt());
  }
}
