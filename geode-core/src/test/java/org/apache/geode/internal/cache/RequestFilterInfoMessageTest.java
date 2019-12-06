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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.cache.InitialImageOperation.RequestFilterInfoMessage;

public class RequestFilterInfoMessageTest {

  private ClusterDistributionManager dm;
  private InternalCache cache;
  private String path;
  private LocalRegion region;

  @Before
  public void setUp() {
    path = "path";

    dm = mock(ClusterDistributionManager.class);
    cache = mock(InternalCache.class);
    region = mock(LocalRegion.class);

    when(dm.getCache()).thenReturn(cache);
    when(cache.getInternalRegionByPath(path)).thenReturn(region);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    RequestFilterInfoMessage mockRequestFilterInfoMessage = mock(RequestFilterInfoMessage.class);
    when(mockRequestFilterInfoMessage.getProcessorType()).thenReturn(1);
    assertThat(mockRequestFilterInfoMessage.getProcessorType()).isEqualTo(1);
  }

  @Test
  public void getsRegionFromCacheFromDM() {
    RequestFilterInfoMessage message = new RequestFilterInfoMessage();
    message.regionPath = path;
    message.process(dm);
    verify(dm, times(1)).getCache();
    verify(cache, times(1)).getInternalRegionByPath(path);
  }
}
