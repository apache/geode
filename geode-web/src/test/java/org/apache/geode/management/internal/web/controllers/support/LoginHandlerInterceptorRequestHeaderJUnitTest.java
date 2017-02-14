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
package org.apache.geode.management.internal.web.controllers.support;


import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.junit.categories.UnitTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;

@Category(UnitTest.class)
public class LoginHandlerInterceptorRequestHeaderJUnitTest {

  @Before
  public void before() {
    LoginHandlerInterceptor.getEnvironment().clear();
  }

  @After
  public void after() {
    LoginHandlerInterceptor.getEnvironment().clear();
  }

  @Test
  public void testCaseInsensitive() throws Exception {
    LoginHandlerInterceptor interceptor = new LoginHandlerInterceptor();
    MockHttpServletRequest mockRequest = new MockHttpServletRequest();
    mockRequest.addHeader("Security-Username", "John");
    mockRequest.addHeader("Security-Password", "Password");
    mockRequest.addHeader("security-something", "anything");
    mockRequest.addHeader("Content-Type", "application/json");

    interceptor.preHandle(mockRequest, null, null);
    Map<String, String> env = interceptor.getEnvironment();

    // make sure only security-* are put in the environment variable
    assertThat(env).hasSize(3);
    assertThat(env.get("security-username")).isEqualTo("John");
    assertThat(env.get("security-password")).isEqualTo("Password");
    assertThat(env.get("security-something")).isEqualTo("anything");
  }

}
