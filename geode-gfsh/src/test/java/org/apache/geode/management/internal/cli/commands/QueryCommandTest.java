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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;

public class QueryCommandTest {
  private final QueryCommand command = new QueryCommand();

  @Test
  public void targetMemberIsNotSetIfMemberOptionsIsNotUsed() {
    QueryCommand spyCommand = spy(command);
    String query = "select query";
    doReturn(mock(DataCommandResult.class)).when(spyCommand).select(query, null);

    spyCommand.query(query, null, false, null);

    verify(spyCommand).select(query, null);
  }

  @Test
  public void targetMemberIsSetIfMemberOptionsIsUsed() {
    QueryCommand spyCommand = spy(command);
    DistributedMember member = mock(DistributedMember.class);
    String query = "select query";
    String memberName = "member";
    doReturn(member).when(spyCommand).getMember(memberName);
    doReturn(mock(DataCommandResult.class)).when(spyCommand).select(query, member);

    spyCommand.query(query, null, false, memberName);

    verify(spyCommand).select(query, member);
  }

  @Test
  public void getQueryRegionsAssociatedMembersInvokedIfNoTargetProvided() {
    QueryCommand spyCommand = spy(command);
    InternalCache cache = mock(InternalCache.class);
    Set<String> regionsInQuery = new HashSet<>();
    doReturn(new HashSet<>()).when(spyCommand).getQueryRegionsAssociatedMembers(cache,
        regionsInQuery);

    spyCommand.getMembers(null, cache, regionsInQuery);

    verify(spyCommand).getQueryRegionsAssociatedMembers(cache, regionsInQuery);
  }

}
