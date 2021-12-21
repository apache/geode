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
package org.apache.geode.management.internal.web.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class ShellCommandsControllerProcessCommandTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ShellCommandsController controller;
  private ResultModel fakeResult;

  @Before
  public void setup() throws IOException {
    controller = spy(ShellCommandsController.class);
  }

  @Test
  public void infoOkResult() throws IOException {
    fakeResult = ResultModel.createInfo("Some info message");
    doReturn(fakeResult.toJson()).when(controller).processCommand(anyString(), any(), any());
    ResponseEntity<InputStreamResource> responseJsonStream = controller.command("xyz", null);
    assertThatContentTypeEquals(responseJsonStream, MediaType.APPLICATION_JSON);

    String responseJson = toString(responseJsonStream);
    ResultModel result = ResultModel.fromJson(responseJson);

    assertThat(result.getInfoSection("info").getContent().get(0))
        .isEqualTo(fakeResult.getInfoSection("info").getContent().get(0));
  }

  @Test
  public void errorResult() throws IOException {
    fakeResult = ResultModel.createError("Some error message");
    doReturn(fakeResult.toJson()).when(controller).processCommand(anyString(), any(), any());
    ResponseEntity<InputStreamResource> responseJsonStream = controller.command("xyz", null);
    assertThatContentTypeEquals(responseJsonStream, MediaType.APPLICATION_JSON);

    String responseJson = toString(responseJsonStream);
    ResultModel result = ResultModel.fromJson(responseJson);

    assertThat(result.getInfoSection("info").getContent().get(0))
        .isEqualTo(fakeResult.getInfoSection("info").getContent().get(0));
  }

  @Test
  public void resultWithFile() throws IOException {
    File tempFile = temporaryFolder.newFile();
    FileUtils.writeStringToFile(tempFile, "some file contents", "UTF-8");

    fakeResult = new ResultModel();
    fakeResult.addFile(tempFile, FileResultModel.FILE_TYPE_FILE);

    doReturn(fakeResult.toJson()).when(controller).processCommand(anyString(), any(), any());
    ResponseEntity<InputStreamResource> responseFileStream = controller.command("xyz", null);

    assertThatContentTypeEquals(responseFileStream, MediaType.APPLICATION_OCTET_STREAM);

    String fileContents = toFileContents(responseFileStream);
    assertThat(fileContents).isEqualTo("some file contents");
  }

  private String toFileContents(ResponseEntity<InputStreamResource> response) throws IOException {
    return IOUtils.toString(response.getBody().getInputStream(), StandardCharsets.UTF_8);
  }

  private String toString(ResponseEntity<InputStreamResource> response) throws IOException {
    return IOUtils.toString(response.getBody().getInputStream(), StandardCharsets.UTF_8);
  }

  private void assertThatContentTypeEquals(ResponseEntity<InputStreamResource> response,
      MediaType mediaType) {
    assertThat(response.getHeaders().get(HttpHeaders.CONTENT_TYPE))
        .containsExactly(mediaType.toString());

  }
}
