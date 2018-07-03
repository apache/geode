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
package org.apache.geode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

public class InstantiatorTest {

  @Test
  public void shouldBeMockable() throws Exception {
    Instantiator mockInstantiator = mock(Instantiator.class);
    EventID mockEventID = mock(EventID.class);
    ClientProxyMembershipID mockClientProxyMembershipID = mock(ClientProxyMembershipID.class);

    when(mockInstantiator.getInstantiatedClass()).thenReturn(null);
    when(mockInstantiator.getId()).thenReturn(0);
    when(mockInstantiator.getEventId()).thenReturn(mockEventID);
    when(mockInstantiator.getContext()).thenReturn(mockClientProxyMembershipID);

    mockInstantiator.setEventId(mockEventID);
    mockInstantiator.setContext(mockClientProxyMembershipID);

    verify(mockInstantiator, times(1)).setEventId(mockEventID);
    verify(mockInstantiator, times(1)).setContext(mockClientProxyMembershipID);

    assertThat(mockInstantiator.getEventId()).isSameAs(mockEventID);
    assertThat(mockInstantiator.getContext()).isSameAs(mockClientProxyMembershipID);
    assertThat(mockInstantiator.getInstantiatedClass()).isNull();
  }
}
