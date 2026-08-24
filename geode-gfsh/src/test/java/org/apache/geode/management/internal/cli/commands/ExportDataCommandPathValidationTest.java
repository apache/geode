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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.Collections;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.rules.GfshParserRule;

/**
 * Tests the path validation and the authorization {@code export data} applies before it sends any
 * work to a member.
 *
 * <p>
 * The directory configuration is applied on the member; the command checks the option for a parent
 * directory reference and asks for the permissions the operation needs.
 *
 * @see ExportDataCommandPermissionsDUnitTest for the permissions end to end in a secured
 *      cluster
 */
public class ExportDataCommandPathValidationTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private static final String REGION = "testRegion";
  /** The region path converter prepends the separator before the command method sees it. */
  private static final String REGION_PATH = Region.SEPARATOR + REGION;

  private ExportDataCommand command;
  private ArgumentCaptor<Object> functionArgsCaptor;

  @Before
  public void before() {
    command = spy(ExportDataCommand.class);

    doNothing().when(command).authorize(any(Resource.class), any(Operation.class), anyString());
    doReturn(mock(DistributedMember.class)).when(command).getMember(anyString());

    CliFunctionResult okResult =
        new CliFunctionResult("server1", CliFunctionResult.StatusState.OK, "exported");
    ResultCollector<?, ?> collector = mock(ResultCollector.class);
    doReturn(Collections.singletonList(okResult)).when(collector).getResult();

    functionArgsCaptor = ArgumentCaptor.forClass(Object.class);
    doReturn(collector).when(command).executeFunction(any(), functionArgsCaptor.capture(),
        any(DistributedMember.class));
  }

  private String capturedExportPath() {
    Object args = functionArgsCaptor.getValue();
    assertThat(args).isInstanceOf(String[].class);
    return ((String[]) args)[1];
  }

  private void verifyNoExportWasRequested() {
    verify(command, never()).executeFunction(any(), any(), any(DistributedMember.class));
  }

  /**
   * A "../" element in --file is refused before anything is sent to a member.
   */
  @Test
  public void parentReferenceInFileOptionIsRejected() {
    parser
        .executeAndAssertThat(command, "export data --member=server1 --region=" + REGION
            + " --file=../../../../var/tmp/snapshot.gfd")
        .statusIsError()
        .containsOutput("must not contain a \"..\" path segment");

    verifyNoExportWasRequested();
  }

  /**
   * A "../" buried in the middle of an otherwise absolute --file is refused too - the check looks
   * at every element of the path, not just its start.
   */
  @Test
  public void parentReferenceInsideAnAbsoluteFilePathIsRejected() {
    parser
        .executeAndAssertThat(command, "export data --member=server1 --region=" + REGION
            + " --file=/var/tmp/subdir/../../snapshot.gfd")
        .statusIsError()
        .containsOutput("must not contain a \"..\" path segment");

    verifyNoExportWasRequested();
  }

  /**
   * The --dir option is checked as well, even though its file name is generated rather than
   * supplied.
   */
  @Test
  public void parentReferenceInDirOptionIsRejected() {
    parser
        .executeAndAssertThat(command,
            "export data --member=server1 --region=" + REGION + " --dir=/tmp/subdir/../../var/tmp")
        .statusIsError()
        .containsOutput("must not contain a \"..\" path segment");

    verifyNoExportWasRequested();
  }

  /**
   * An ordinary path is forwarded unchanged, for the member to resolve.
   */
  @Test
  public void ordinaryPathIsForwardedToTheMember() {
    parser
        .executeAndAssertThat(command,
            "export data --member=server1 --region=" + REGION + " --file=/var/tmp/snapshot.gfd")
        .statusIsSuccess();

    assertThat(capturedExportPath()).isEqualTo("/var/tmp/snapshot.gfd");
  }

  /**
   * Same for --dir, with the generated file name appended.
   */
  @Test
  public void ordinaryDirectoryIsForwardedToTheMember() {
    parser
        .executeAndAssertThat(command,
            "export data --member=server1 --region=" + REGION + " --dir=/var/tmp")
        .statusIsSuccess();

    assertThat(capturedExportPath()).isEqualTo(new File("/var/tmp", REGION + ".gfd").getPath());
  }

  /**
   * The extension check still applies.
   */
  @Test
  public void fileExtensionIsValidated() {
    parser
        .executeAndAssertThat(command,
            "export data --member=server1 --region=" + REGION + " --file=/var/tmp/snapshot.txt")
        .statusIsError()
        .containsOutput("Invalid file type, the file extension must be \".gfd\"");

    verifyNoExportWasRequested();
  }

  /**
   * Writing a file on a member's host needs a cluster write permission, alongside read access to
   * the data being exported.
   */
  @Test
  public void exportRequiresClusterWriteAndDataRead() {
    parser
        .executeAndAssertThat(command,
            "export data --member=server1 --region=" + REGION + " --file=/var/tmp/snapshot.gfd")
        .statusIsSuccess();

    verify(command).authorize(Resource.DATA, Operation.READ, REGION_PATH);
    verify(command).authorize(Resource.CLUSTER, Operation.WRITE, ResourcePermission.ALL);
  }

  /**
   * Permissions are checked before any work is done, so the export is not sent to the member.
   */
  @Test
  public void clusterWriteIsCheckedBeforeTheExportIsSent() {
    doThrow(new NotAuthorizedException("dataRead not authorized for CLUSTER:WRITE"))
        .when(command).authorize(eq(Resource.CLUSTER), eq(Operation.WRITE), anyString());

    assertThatThrownBy(
        () -> command.exportData("server1", REGION_PATH, "/var/tmp/snapshot.gfd", null, false))
            .isInstanceOf(NotAuthorizedException.class)
            .hasMessageContaining("CLUSTER:WRITE");

    verifyNoExportWasRequested();
  }
}
