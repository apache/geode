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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.apache.geode.test.junit.assertions.ResultModelAssert.assertResultModel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class CreateMappingCommandInterceptorTest {

  private final CreateMappingCommand.Interceptor interceptor =
      new CreateMappingCommand.Interceptor();

  private final GfshParseResult gfshParseResult = mock(GfshParseResult.class);

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void preExecutionGivenNullPdxClassFileReturnsOK() {
    when(gfshParseResult.getParamValue(MappingConstants.PDX_CLASS_FILE)).thenReturn(null);
    ResultModel result = interceptor.preExecution(gfshParseResult);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
  }

  @Test
  public void preExecutionGivenNonExistingPdxClassFileReturnsError() {
    when(gfshParseResult.getParamValue(MappingConstants.PDX_CLASS_FILE))
        .thenReturn("NonExistingFile");
    ResultModel result = interceptor.preExecution(gfshParseResult);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertResultModel(result).hasInfoResultModel("info").hasOutput()
        .contains("NonExistingFile not found.");
  }

  @Test
  public void preExecutionGivenDirectoryAsPdxClassFileReturnsError() throws IOException {
    File tempFolder = testFolder.newFolder("tempFolder");
    when(gfshParseResult.getParamValue(MappingConstants.PDX_CLASS_FILE))
        .thenReturn(tempFolder.getAbsolutePath());
    ResultModel result = interceptor.preExecution(gfshParseResult);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertResultModel(result).hasInfoResultModel("info").hasOutput()
        .contains(tempFolder.getAbsolutePath() + " is not a file.");
  }

  @Test
  public void preExecutionGivenFileWithoutExtensionAsPdxClassFileReturnsError() throws IOException {
    File tempFile = testFolder.newFile("tempFile");
    when(gfshParseResult.getParamValue(MappingConstants.PDX_CLASS_FILE))
        .thenReturn(tempFile.getAbsolutePath());
    ResultModel result = interceptor.preExecution(gfshParseResult);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertResultModel(result).hasInfoResultModel("info").hasOutput()
        .contains(tempFile.getAbsolutePath() + " must end with \".jar\" or \".class\".");
  }

  @Test
  public void preExecutionGivenClassFileAsPdxClassFileReturnsOK() throws IOException {
    File tempFile = testFolder.newFile("tempFile.class");
    when(gfshParseResult.getParamValue(MappingConstants.PDX_CLASS_FILE))
        .thenReturn(tempFile.getAbsolutePath());
    ResultModel result = interceptor.preExecution(gfshParseResult);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getFileList()).containsExactly(tempFile);
  }

  @Test
  public void preExecutionGivenJarFileAsPdxClassFileReturnsOK() throws IOException {
    File tempFile = testFolder.newFile("tempFile.jar");
    when(gfshParseResult.getParamValue(MappingConstants.PDX_CLASS_FILE))
        .thenReturn(tempFile.getAbsolutePath());
    ResultModel result = interceptor.preExecution(gfshParseResult);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getFileList()).containsExactly(tempFile);
  }

}
