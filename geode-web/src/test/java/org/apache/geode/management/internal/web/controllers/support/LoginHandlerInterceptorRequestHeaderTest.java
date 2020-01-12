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
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({GfshTest.class, SecurityTest.class, LoggingTest.class})
public class LoginHandlerInterceptorRequestHeaderTest {

  private SecurityService securityService;
  private LoginHandlerInterceptor interceptor;

  @Before
  public void before() {
    LoginHandlerInterceptor.getEnvironment().clear();
    securityService = Mockito.mock(SecurityService.class);
    interceptor = new LoginHandlerInterceptor(securityService);
  }

  @After
  public void after() {
    LoginHandlerInterceptor.getEnvironment().clear();
  }

  @Test
  public void testCaseInsensitive() throws Exception {
    MockHttpServletRequest mockRequest = new MockHttpServletRequest();
    mockRequest.addHeader("Security-Username", "John");
    mockRequest.addHeader("Security-Password", "Password");
    mockRequest.addHeader("security-something", "anything");
    mockRequest.addHeader("Content-Type", "application/json");

    interceptor.preHandle(mockRequest, null, null);

    ArgumentCaptor<Properties> props = ArgumentCaptor.forClass(Properties.class);
    verify(securityService).login(props.capture());
    assertThat(props.getValue().getProperty("security-username")).isEqualTo("John");
    assertThat(props.getValue().getProperty("security-password")).isEqualTo("Password");

    Map<String, String> env = interceptor.getEnvironment();
    // make sure security-* are not put in the environment variable
    assertThat(env).hasSize(0);
  }
}
