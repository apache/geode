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
package org.apache.geode.management.internal.cli.commands;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

public class GfshCommandJUnitTest {

  private GfshCommand command;

  @Before
  public void before() throws Exception {
    command = spy(GfshCommand.class);
  }

  @Test
  public void getMember() throws Exception {
    doReturn(null).when(command).findMember("test");
    assertThatThrownBy(() -> command.getMember("test")).isInstanceOf(EntityNotFoundException.class);
  }

  @Test
  public void getMembers() throws Exception {
    String[] members = {"member"};
    doReturn(Collections.emptySet()).when(command).findMembers(members, null);
    assertThatThrownBy(() -> command.getMembers(members, null))
        .isInstanceOf(EntityNotFoundException.class);
  }

  @Test
  public void getMembersIncludingLocators() throws Exception {
    String[] members = {"member"};
    doReturn(Collections.emptySet()).when(command).findMembersIncludingLocators(members, null);
    assertThatThrownBy(() -> command.getMembersIncludingLocators(members, null))
        .isInstanceOf(EntityNotFoundException.class);
  }

  @Test
  public void findAllOtherLocators() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    when(member1.getId()).thenReturn("member1");
    when(member2.getId()).thenReturn("member2");
    DistributedSystem system = mock(DistributedSystem.class);
    when(system.getDistributedMember()).thenReturn(member1);
    when(cache.getDistributedSystem()).thenReturn(system);
    Set<DistributedMember> members = new HashSet<>(asList(member1, member2));
    doReturn(members).when(command).findAllLocators();

    assertThat(command.findAllOtherLocators())
        .extracting(DistributedMember::getId)
        .containsExactly("member2");
  }
}
