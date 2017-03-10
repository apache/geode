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

import org.apache.commons.io.IOUtils;
import org.apache.geode.management.internal.cli.CommandResponseBuilder;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

@Category(UnitTest.class)
public class ExportLogControllerTest {
  private ExportLogController controller;

  @Before
  public void before() throws Exception {
    controller = new ExportLogController();
  }

  @Test
  public void testErrorResponse() throws Exception {
    String message = "No Members Found";
    CommandResult result = (CommandResult) ResultBuilder.createUserErrorResult(message);
    String responseJson = CommandResponseBuilder.createCommandResponseJson("memberName", result);

    ResponseEntity<InputStreamResource> resp = controller.getResponse(responseJson);
    HttpHeaders headers = resp.getHeaders();
    assertThat(headers.get(HttpHeaders.CONTENT_TYPE).get(0))
        .isEqualTo(MediaType.APPLICATION_JSON_VALUE);

    InputStreamResource body = resp.getBody();
    assertThat(IOUtils.toString(body.getInputStream(), "utf-8")).isEqualTo(responseJson);
  }
}
