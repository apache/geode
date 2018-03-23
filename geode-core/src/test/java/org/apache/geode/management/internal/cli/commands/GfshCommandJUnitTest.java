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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GfshCommandJUnitTest {

  private GfshCommand command;
  private Gfsh gfsh;
  private InternalClusterConfigurationService clusterConfigurationService;

  @Before
  public void before() throws Exception {
    command = spy(GfshCommand.class);
    gfsh = mock(Gfsh.class);
    clusterConfigurationService = mock(InternalClusterConfigurationService.class);
  }

  @Test
  public void isConnectedAndReady() throws Exception {
    when(command.getGfsh()).thenReturn(null);
    assertThat(command.isConnectedAndReady()).isFalse();

    when(command.getGfsh()).thenReturn(gfsh);
    when(gfsh.isConnectedAndReady()).thenReturn(false);
    assertThat(command.isConnectedAndReady()).isFalse();

    when(command.getGfsh()).thenReturn(gfsh);
    when(gfsh.isConnectedAndReady()).thenReturn(true);
    assertThat(command.isConnectedAndReady()).isTrue();
  }

  @Test
  public void persistClusterConfiguration() throws Exception {
    when(command.getConfigurationService()).thenReturn(null);
    Result result = ResultBuilder.createInfoResult("info");
    Runnable runnable = mock(Runnable.class);

    command.persistClusterConfiguration(result, runnable);
    assertThat(result.failedToPersist()).isTrue();

    when(command.getConfigurationService()).thenReturn(clusterConfigurationService);
    command.persistClusterConfiguration(result, runnable);
    assertThat(result.failedToPersist()).isFalse();
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
}
