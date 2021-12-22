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

import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateDiskStoreCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateDiskStoreCommand command;

  @Before
  public void before() throws Exception {
    command = spy(CreateDiskStoreCommand.class);

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());
    doReturn(Collections.singletonList(mock(CliFunctionResult.class))).when(command)
        .executeAndGetFunctionResult(any(), any(), any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
        any());
    doReturn(true).when(command).waitForDiskStoreMBeanCreation(any(), any());
  }

  @Test
  public void dirWithRelativePath() throws Exception {
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist")
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getContent().replace('\\', '/')).isEqualTo("./data/persist");
  }

  @Test
  public void dirWithAbsolutePath() throws Exception {
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist")
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getContent().replace('\\', '/')).isEqualTo("/data/persist");
  }

  @Test
  public void dirIsCreatedWithExpectedSpecifiedSize() throws Exception {
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist#32768")
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getDirSize()).isEqualTo("32768");
  }

  @Test
  public void dirIsCreatedWithExpectedDefaultSize() {
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist")
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getDirSize()).isEqualTo(String.valueOf(Integer.MAX_VALUE));
  }

  @Test
  public void commandFailsIfDirSizeIsOverTheMaximum() {
    long invalidValue = (long) Integer.MAX_VALUE + 1;
    gfsh.executeAndAssertThat(command,
        "create disk-store --name=ds1 --dir=/data/persist#" + invalidValue)
        .statusIsError()
        .containsOutput("Directory size (2147483648) is over the maximum allowed value");
  }

  @Test
  public void commandFailsIfDirSizeIsNegative() {
    gfsh.executeAndAssertThat(command,
        "create disk-store --name=ds1 --dir=/data/persist#-1024")
        .statusIsError()
        .containsOutput("Directory size cannot be negative (-1024)");
  }

  @Test
  public void commandFailsIfDirSizeIsNotANumber() {
    gfsh.executeAndAssertThat(command,
        "create disk-store --name=ds1 --dir=/data/persist#123ABC")
        .statusIsError()
        .containsOutput("Incorrect directory size specified (123ABC)");
  }
}
