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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ListMembersCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ListMembersCommand command;
  private DistributedMember member1;
  private DistributedMember member2;
  private Set<DistributedMember> members;

  @Before
  public void before() {
    command = spy(ListMembersCommand.class);
    members = new HashSet<>();
    doReturn(members).when(command).findMembersIncludingLocators(any(), any());

    member1 = mock(DistributedMember.class);
    when(member1.getName()).thenReturn("name");
    when(member1.getId()).thenReturn("id");
    when(member1.getUniqueId()).thenReturn("uniqueId1");
    doReturn("uniqueId1").when(command).getCoordinatorId();

    member2 = mock(DistributedMember.class);
    when(member2.getName()).thenReturn("name2");
    when(member2.getId()).thenReturn("id2");
    when(member2.getUniqueId()).thenReturn("uniqueId2");

    // This will enforce the sort order in TreeSet used by ListMembersCommand.
    when(member1.compareTo(member2)).thenReturn(-1);
    when(member2.compareTo(member1)).thenReturn(1);
  }

  @Test
  public void listMembersNoMemberFound() {
    gfsh.executeAndAssertThat(command, "list members").containsOutput("No Members Found")
        .statusIsSuccess();
  }

  @Test
  public void basicListMembers() {
    members.add(member1);

    Map<String, List<String>> table = gfsh.executeAndAssertThat(command, "list members")
        .hasTableSection().getActual().getContent();

    assertThat(table.get("Name")).contains("name");
    assertThat(table.get("Id")).contains("id [Coordinator]");
  }

  @Test
  public void noCoordinator() {
    members.add(member1);
    doReturn(null).when(command).getCoordinatorId();

    Map<String, List<String>> table = gfsh.executeAndAssertThat(command, "list members")
        .hasTableSection().getActual().getContent();

    assertThat(table.get("Name")).contains("name");
    assertThat(table.get("Id")).contains("id");
  }

  @Test
  public void listMembersMultipleItems() {
    members.add(member1);
    members.add(member2);

    Map<String, List<String>> table = gfsh.executeAndAssertThat(command, "list members")
        .hasTableSection().getActual().getContent();

    assertThat(table.get("Name")).contains("name", "name2");
    assertThat(table.get("Id")).contains("id [Coordinator]", "id2");
  }
}
