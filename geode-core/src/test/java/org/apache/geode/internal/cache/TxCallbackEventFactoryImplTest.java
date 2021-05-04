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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class TxCallbackEventFactoryImplTest {
  private TxCallbackEventFactoryImpl factory;
  private InternalRegion region;
  private FilterRoutingInfo routingInfo;
  private EntryEventImpl entryEvent;

  @Before
  public void setup() {
    factory = new TxCallbackEventFactoryImpl();
    region = mock(InternalRegion.class);
    routingInfo = mock(FilterRoutingInfo.class);
    entryEvent = mock(EntryEventImpl.class);
  }

  @Test
  public void setLocalFilterInfoNotInvokedIfNoFilterRoutingInfoAvailable() {
    factory.setLocalFilterInfo(region, null, entryEvent);

    verify(entryEvent, never()).setLocalFilterInfo(any());
  }

  @Test
  public void setLocalFilterInfoInvokedIfFoundMatchingFilterInfo() {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    FilterRoutingInfo.FilterInfo localRouting = mock(FilterRoutingInfo.FilterInfo.class);
    when(region.getMyId()).thenReturn(member);
    when(routingInfo.getFilterInfo(member)).thenReturn(localRouting);

    factory.setLocalFilterInfo(region, routingInfo, entryEvent);

    verify(entryEvent).setLocalFilterInfo(localRouting);
  }
}
