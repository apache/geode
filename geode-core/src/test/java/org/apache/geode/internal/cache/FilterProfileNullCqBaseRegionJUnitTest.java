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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;

public class FilterProfileNullCqBaseRegionJUnitTest {

  private FilterProfile filterProfile;
  private CqService mockCqService;
  private GemFireCacheImpl mockCache;
  private CqStateImpl cqState;
  private ServerCQ serverCQ;

  @Before
  public void setUp() {
    mockCache = mock(GemFireCacheImpl.class);
    mockCqService = mock(CqService.class);
    cqState = mock(CqStateImpl.class);
    serverCQ = mock(ServerCQ.class);

    when(mockCache.getCqService()).thenReturn(mockCqService);
    doNothing().when(mockCqService).start();
    when(mockCache.getCacheServers()).thenReturn(Collections.emptyList());
    when(serverCQ.getState()).thenReturn(cqState);
    when(serverCQ.getCqBaseRegion()).thenReturn(null);

  }

  @Test
  public void whenCqBaseRegionIsNullThenTheCqShouldNotBeAddedToTheCqMap()
      throws CqException, RegionNotFoundException {
    doThrow(RegionNotFoundException.class).when(serverCQ).registerCq(eq(null), eq(null), anyInt());

    filterProfile = new FilterProfile();
    filterProfile.processRegisterCq("TestCq", serverCQ, true, mockCache);
    assertEquals(0, filterProfile.getCqMap().size());
    filterProfile.processCloseCq("TestCq");

  }
}
