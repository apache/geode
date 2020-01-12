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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionAdvisor.ProfileId;
import org.apache.geode.internal.cache.GridAdvisor.GridProfile;
import org.apache.geode.test.fake.Fakes;

public class GridProfileTest {

  @Test
  public void shouldBeMockable() throws Exception {
    GridProfile mockGridProfile = mock(GridProfile.class);
    InternalCache cache = Fakes.cache();
    ProfileId mockProfileId = mock(ProfileId.class);
    List<Profile> listOfProfiles = new ArrayList<>();
    listOfProfiles.add(mock(Profile.class));

    when(mockGridProfile.getHost()).thenReturn("HOST");
    when(mockGridProfile.getPort()).thenReturn(1);
    when(mockGridProfile.getId()).thenReturn(mockProfileId);

    mockGridProfile.setHost("host");
    mockGridProfile.setPort(2);
    mockGridProfile.tellLocalControllers(true, true, listOfProfiles);
    mockGridProfile.tellLocalBridgeServers(cache, true, true, listOfProfiles);

    verify(mockGridProfile, times(1)).setHost("host");
    verify(mockGridProfile, times(1)).setPort(2);
    verify(mockGridProfile, times(1)).tellLocalControllers(true, true, listOfProfiles);
    verify(mockGridProfile, times(1)).tellLocalBridgeServers(cache, true, true, listOfProfiles);

    assertThat(mockGridProfile.getHost()).isEqualTo("HOST");
    assertThat(mockGridProfile.getPort()).isEqualTo(1);
    assertThat(mockGridProfile.getId()).isSameAs(mockProfileId);
  }
}
