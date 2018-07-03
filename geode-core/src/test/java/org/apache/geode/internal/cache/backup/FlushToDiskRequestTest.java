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
package org.apache.geode.internal.cache.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;

public class FlushToDiskRequestTest {

  private FlushToDiskRequest flushToDiskRequest;

  private DistributionManager dm;
  private Set<InternalDistributedMember> recipients;
  private int msgId;
  private FlushToDiskFactory flushToDiskFactory;
  private InternalDistributedMember sender;
  private InternalCache cache;
  private FlushToDisk flushToDisk;

  @Before
  public void setUp() throws Exception {
    dm = mock(DistributionManager.class);
    sender = mock(InternalDistributedMember.class);
    cache = mock(InternalCache.class);
    flushToDiskFactory = mock(FlushToDiskFactory.class);
    flushToDisk = mock(FlushToDisk.class);

    msgId = 42;
    recipients = new HashSet<>();

    when(dm.getCache()).thenReturn(cache);
    when(dm.getDistributionManagerId()).thenReturn(sender);
    when(flushToDiskFactory.createFlushToDisk(eq(cache))).thenReturn(flushToDisk);
    when(flushToDiskFactory.createResponse(eq(sender))).thenReturn(mock(FlushToDiskResponse.class));

    flushToDiskRequest = new FlushToDiskRequest(sender, recipients, msgId, flushToDiskFactory);
  }

  @Test
  public void usesFactoryToCreateFlushToDisk() throws Exception {
    flushToDiskRequest.createResponse(dm);

    verify(flushToDiskFactory, times(1)).createFlushToDisk(eq(cache));
  }

  @Test
  public void usesFactoryToCreateResponse() throws Exception {
    flushToDiskRequest.createResponse(dm);

    verify(flushToDiskFactory, times(1)).createResponse(eq(sender));
  }

  @Test
  public void returnsFlushToDiskResponse() throws Exception {
    assertThat(flushToDiskRequest.createResponse(dm)).isInstanceOf(FlushToDiskResponse.class);
  }
}
