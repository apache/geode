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

    // Create a mocked CliFunctionResult that returns true for isSuccessful()
    CliFunctionResult mockResult = mock(CliFunctionResult.class);
    doReturn(true).when(mockResult).isSuccessful();
    doReturn(Collections.singletonList(mockResult)).when(command)
        .executeAndGetFunctionResult(any(), any(), any());
    doReturn(Pair.of(Boolean.TRUE, null)).when(command).validateDiskstoreAttributes(any(),
        any());
    doReturn(true).when(command).waitForDiskStoreMBeanCreation(any(), any());
  }

  @Test
  public void dirWithRelativePath() throws Exception {
    // Shell 3.x: Explicitly verify command succeeds (mock now returns successful result)
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=./data/persist")
            .statusIsSuccess()
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getContent().replace('\\', '/')).isEqualTo("./data/persist");
  }

  @Test
  public void dirWithAbsolutePath() throws Exception {
    // Shell 3.x: Explicitly verify command succeeds (mock now returns successful result)
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist")
            .statusIsSuccess()
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getContent().replace('\\', '/')).isEqualTo("/data/persist");
  }

  @Test
  public void dirIsCreatedWithExpectedSpecifiedSize() throws Exception {
    // Shell 3.x: Explicitly verify command succeeds (mock now returns successful result)
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist#32768")
            .statusIsSuccess()
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getDirSize()).isEqualTo("32768");
  }

  @Test
  public void dirIsCreatedWithExpectedDefaultSize() {
    // Shell 3.x: Explicitly verify command succeeds (mock now returns successful result)
    ResultModel resultModel =
        gfsh.executeAndAssertThat(command, "create disk-store --name=ds1 --dir=/data/persist")
            .statusIsSuccess()
            .getResultModel();
    DiskStoreType diskStoreType = (DiskStoreType) resultModel.getConfigObject();

    DiskDirType diskDirType = diskStoreType.getDiskDirs().get(0);
    assertThat(diskDirType.getDirSize()).isEqualTo(String.valueOf(Integer.MAX_VALUE));
  }

  @Test
  public void commandFailsIfDirSizeIsOverTheMaximum() {
    // Shell 3.x: Partial error message match (error messages may vary between Shell versions)
    long invalidValue = (long) Integer.MAX_VALUE + 1;
    gfsh.executeAndAssertThat(command,
        "create disk-store --name=ds1 --dir=/data/persist#" + invalidValue)
        .statusIsError()
        .containsOutput("over the maximum");
  }

  @Test
  public void commandFailsIfDirSizeIsNegative() {
    // Shell 3.x: Partial error message match
    gfsh.executeAndAssertThat(command,
        "create disk-store --name=ds1 --dir=/data/persist#-1024")
        .statusIsError()
        .containsOutput("cannot be negative");
  }

  @Test
  public void commandFailsIfDirSizeIsNotANumber() {
    // Shell 3.x: Partial error message match
    gfsh.executeAndAssertThat(command,
        "create disk-store --name=ds1 --dir=/data/persist#123ABC")
        .statusIsError()
        .containsOutput("Incorrect directory size");
  }
}
