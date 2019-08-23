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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;



import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateDiskStoreCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateDiskStoreCommand command;

  @Before
  public void before() throws Exception {
    command = spy(CreateDiskStoreCommand.class);
  }

  @Test
  public void dirWithRelativePath() throws Exception {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());
    doReturn(Collections.singletonList(mock(CliFunctionResult.class))).when(command)
        .executeAndGetFunctionResult(any(), any(), any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
        any());
    doReturn(true).when(command).waitForDiskStoreMBeanCreation(any(), any());
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist")
            .getResultModel();
    verifyDiskStoreDir(resultModel, "./data/persist");
  }

  @Test
  public void dirWithAbsolutePath() throws Exception {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());
    doReturn(Collections.singletonList(mock(CliFunctionResult.class))).when(command)
        .executeAndGetFunctionResult(any(), any(), any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
        any());
    doReturn(true).when(command).waitForDiskStoreMBeanCreation(any(), any());
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist")
            .getResultModel();
    verifyDiskStoreDir(resultModel, "/data/persist");

  }

  @Test
  public void stageConfiguration_doesNotExecuteCreateDiskStoreFunction() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(),
            any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembersIncludingLocators(any(),
            any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
            any());

    ResultModel resultModel =
            gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist --stage-configuration=true")
                    .statusIsSuccess().getResultModel();
    verifyDiskStoreDir(resultModel, "./data/persist");

    verify(command, never()).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void stageConfiguration_noServers_persistsConfig() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(),
            any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembersIncludingLocators(any(),
            any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
            any());

    ResultModel resultModel =
            gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist --stage-configuration=true")
                    .statusIsSuccess().getResultModel();
    verifyDiskStoreDir(resultModel, "./data/persist");
  }

  @Test
  public void stageConfiguration_withServers_persistsConfig() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(),
            any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembersIncludingLocators(any(),
            any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
            any());

    ResultModel resultModel =
            gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist --stage-configuration=true")
                    .statusIsSuccess().getResultModel();
    verifyDiskStoreDir(resultModel, "./data/persist");
  }

  @Test
  public void stageConfiguration_isIdempotent() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(),
            any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembersIncludingLocators(any(),
            any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
            any());

    ResultModel resultModel =
            gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist --stage-configuration=true")
                    .statusIsSuccess().getResultModel();

    verifyDiskStoreDir(resultModel, "./data/persist");
    DiskStoreType diskStoreType;
    DiskDirType diskDirType;

    resultModel =
            gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist --stage-configuration=true")
                    .statusIsSuccess().getResultModel();
    verifyDiskStoreDir(resultModel, "./data/persist");
  }

  private void verifyDiskStoreDir(ResultModel resultModel, String expectedDir) {
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getContent().replace('\\', '/')).isEqualTo(expectedDir);
  }
}
