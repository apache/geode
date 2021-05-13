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
package org.apache.geode.cache.wan.internal.client.locator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.internal.admin.remote.DistributionLocatorId;

public class DistributedLocatorsRunnableTest {

  private AutoCloseable mocks;

  @Mock
  LocatorMembershipListenerImpl.DistributeLocatorsRunnable dlr;

  @Before
  public void before() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @After
  public void after() throws Exception {
    mocks.close();
  }

  @Test
  public void notifyRemoteLocatorAndJoiningLocatorDoesNotSendAnyMessageIfBothLocatorsAreTheSame() {
    DistributionLocatorId id = new DistributionLocatorId(40404, "localhost", null);
    dlr.notifyRemoteLocatorAndJoiningLocator(id, 1, id, null);
    verify(dlr, times(0)).sendMessage(any(), any(), any());
  }

  @Test
  public void notifyRemoteLocatorAndJoiningLocatorSendsMessagesToBothLocatorsIfTheyAreDifferent() {
    DistributionLocatorId id1 = new DistributionLocatorId(40404, "localhost", "locator1");
    DistributionLocatorId id2 = new DistributionLocatorId(40406, "localhost", "locator2");
    doCallRealMethod().when(dlr).notifyRemoteLocatorAndJoiningLocator(id1, 1, id2, null);
    doNothing().when(dlr).sendMessage(any(), any(), any());

    dlr.notifyRemoteLocatorAndJoiningLocator(id1, 1, id2, null);

    verify(dlr, times(1)).sendMessage(eq(id1), any(), any());
    verify(dlr, times(1)).sendMessage(eq(id2), any(), any());
  }
}
