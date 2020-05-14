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

package org.apache.geode.management.internal.rest.controllers;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

public class DeploymentManagementControllerTest {

  private DeploymentManagementController controller;

  @Before
  public void before() throws Exception {
    controller = new DeploymentManagementController();
  }

  // this test is to make sure that controller.deploy method can only take a json string
  // as the 2nd parameter, it should not use @RequestPart that depend on HttpMessageConverters to
  // convert the string to a Deployment object because some rest client (curl or postman) won't be
  // able to send in the request successfully if annotated with @RequestPart
  @Test
  public void canOnlyAcceptStringAndFile() throws Exception {
    Method deploy =
        DeploymentManagementController.class.getMethod("deploy", MultipartFile.class, String.class);
    assertThat(deploy).isNotNull();
    Annotation[][] parameterAnnotations = deploy.getParameterAnnotations();
    assertThat(parameterAnnotations[0][1]).isInstanceOf(RequestParam.class);
    assertThat(parameterAnnotations[1][1]).isInstanceOf(RequestParam.class);
  }
}
