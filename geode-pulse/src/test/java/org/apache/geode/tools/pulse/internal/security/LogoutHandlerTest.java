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
package org.apache.geode.tools.pulse.internal.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Repository.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PowerMockIgnore({"javax.management.*", "javax.security.*", "*.UnitTest"})
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@Category({PulseTest.class, SecurityTest.class, LoggingTest.class})
public class LogoutHandlerTest {

  private static final String mockUser = "admin";

  private Repository repository;
  private LogoutHandler handler;

  @Parameter
  public static Authentication authentication;

  @Parameters(name = "{0}")
  public static Collection<Authentication> authentications() throws Exception {
    Authentication defaultAuthentication = mock(Authentication.class, "Default Authentication");
    when(defaultAuthentication.getName()).thenReturn(mockUser);

    GemFireAuthentication gemfireAuthentication =
        mock(GemFireAuthentication.class, "GemFire Authentication");
    when(gemfireAuthentication.getName()).thenReturn(mockUser);

    return Arrays.asList(defaultAuthentication, gemfireAuthentication);
  }

  @Before
  public void setup() throws Exception {
    Cluster cluster = Mockito.spy(Cluster.class);
    repository = Mockito.spy(Repository.class);
    spy(Repository.class);
    when(Repository.class, "get").thenReturn(repository);
    doReturn(cluster).when(repository).getCluster();
    handler = new LogoutHandler("/defaultTargetUrl");
  }

  @Test
  public void testNullAuthentication() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    handler.onLogoutSuccess(request, response, null);

    assertThat(response.getStatus()).isEqualTo(302);
    assertThat(response.getHeader("Location")).isEqualTo("/defaultTargetUrl");
  }

  @Test
  public void testNotNullAuthentication() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    handler.onLogoutSuccess(request, response, authentication);

    assertThat(response.getStatus()).isEqualTo(302);
    assertThat(response.getHeader("Location")).isEqualTo("/defaultTargetUrl");
    verify(repository, Mockito.times(1)).logoutUser(mockUser);
  }
}
