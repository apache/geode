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

import java.io.File;
import java.io.IOException;
import java.util.Map;

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
import org.springframework.web.multipart.MultipartFile;

import org.apache.geode.management.internal.cli.CommandResponseBuilder;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.LegacyCommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;

public class ShellCommandsControllerProcessCommandTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ShellCommandsController controller;
  private CommandResult fakeResult;

  @Before
  public void setup() {

    controller = new ShellCommandsController() {
      @Override
      protected String processCommand(String command, final Map<String, String> environment,
          MultipartFile[] fileData) {
        return CommandResponseBuilder.createCommandResponseJson("someMember", fakeResult);
      }
    };
  }

  @Test
  public void infoOkResult() throws IOException {
    fakeResult = new LegacyCommandResult(new InfoResultData("Some info message"));

    ResponseEntity<InputStreamResource> responseJsonStream = controller.command("xyz", null);
    assertThatContentTypeEquals(responseJsonStream, MediaType.APPLICATION_JSON);

    String responseJson = toString(responseJsonStream);
    CommandResult result = ResultBuilder.fromJson(responseJson);

    assertThat(result.nextLine()).isEqualTo(fakeResult.nextLine());
  }

  @Test
  public void errorResult() throws IOException {
    ErrorResultData errorResultData = new ErrorResultData("Some error message");
    fakeResult = new LegacyCommandResult(errorResultData);

    ResponseEntity<InputStreamResource> responseJsonStream = controller.command("xyz", null);
    assertThatContentTypeEquals(responseJsonStream, MediaType.APPLICATION_JSON);

    String responseJson = toString(responseJsonStream);
    CommandResult result = ResultBuilder.fromJson(responseJson);

    assertThat(result.nextLine()).isEqualTo(fakeResult.nextLine());
  }

  @Test
  public void resultWithFile() throws IOException {
    File tempFile = temporaryFolder.newFile();
    FileUtils.writeStringToFile(tempFile, "some file contents", "UTF-8");

    fakeResult = new LegacyCommandResult(tempFile.toPath());

    ResponseEntity<InputStreamResource> responseFileStream = controller.command("xyz", null);

    assertThatContentTypeEquals(responseFileStream, MediaType.APPLICATION_OCTET_STREAM);

    String fileContents = toFileContents(responseFileStream);
    assertThat(fileContents).isEqualTo("some file contents");
  }

  private String toFileContents(ResponseEntity<InputStreamResource> response) throws IOException {
    return IOUtils.toString(response.getBody().getInputStream(), "UTF-8");
  }

  private String toString(ResponseEntity<InputStreamResource> response) throws IOException {
    return IOUtils.toString(response.getBody().getInputStream(), "UTF-8");
  }

  private void assertThatContentTypeEquals(ResponseEntity<InputStreamResource> response,
      MediaType mediaType) {
    assertThat(response.getHeaders().get(HttpHeaders.CONTENT_TYPE))
        .containsExactly(mediaType.toString());

  }
}
