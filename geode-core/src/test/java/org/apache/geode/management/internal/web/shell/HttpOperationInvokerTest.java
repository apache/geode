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

package org.apache.geode.management.internal.web.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpResponse;

import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class HttpOperationInvokerTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private HttpOperationInvoker invoker;
  private ClientHttpResponse response;

  @Before
  public void setup() {}

  @Test
  public void extractResponseOfJsonString() throws Exception {
    String responseString = "my response";
    invoker = new HttpOperationInvoker();
    response =
        new MockClientHttpResponse(IOUtils.toInputStream(responseString, "UTF-8"), HttpStatus.OK);

    response.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
    Object result = invoker.extractResponse(response);
    assertThat(result).isEqualTo(responseString);
  }

  @Test
  public void extractResponseOfFileDownload() throws Exception {
    File responseFile = temporaryFolder.newFile();
    FileUtils.writeStringToFile(responseFile, "some file contents", "UTF-8");
    invoker = new HttpOperationInvoker();
    response = new MockClientHttpResponse(new FileInputStream(responseFile), HttpStatus.OK);
    response.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE);
    Object result = invoker.extractResponse(response);
    Path fileResult = (Path) result;
    assertThat(fileResult).hasSameContentAs(responseFile.toPath());
  }
}
