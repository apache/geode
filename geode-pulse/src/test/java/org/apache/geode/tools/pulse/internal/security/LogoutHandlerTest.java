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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.context.ApplicationContext;
import org.springframework.security.core.Authentication;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.tools.pulse.internal.data.Repository;

@Category({PulseTest.class, SecurityTest.class, LoggingTest.class})
public class LogoutHandlerTest {
  private static final String EXPECTED_REDIRECT_URL = "/defaultTargetUrl";

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private HttpServletRequest request;
  @Mock
  private HttpServletResponse response;
  @Mock
  private Repository repository;
  @Mock
  private ApplicationContext applicationContext;

  private final LogoutHandler handler = new LogoutHandler(EXPECTED_REDIRECT_URL);

  @Before
  public void setup() {
    when(request.getContextPath()).thenReturn("");
    when(response.encodeRedirectURL(EXPECTED_REDIRECT_URL)).thenReturn(EXPECTED_REDIRECT_URL);
    handler.setApplicationContext(applicationContext);
  }

  @Test
  public void onLogoutSuccess_logsOutAuthenticatedUser() throws Exception {
    String authenticatedUser = "authenticated-user";

    Authentication authentication = mock(Authentication.class);
    when(authentication.getName()).thenReturn(authenticatedUser);
    when(applicationContext.getBean("repository", Repository.class)).thenReturn(repository);

    handler.onLogoutSuccess(request, response, authentication);

    verify(repository, times(1)).logoutUser(authenticatedUser);
  }

  @Test
  public void onLogoutSuccess_redirectsToSpecifiedUrl() throws Exception {
    when(applicationContext.getBean("repository", Repository.class)).thenReturn(repository);
    handler.onLogoutSuccess(request, response, mock(Authentication.class));

    verify(response).sendRedirect(EXPECTED_REDIRECT_URL);
  }

  @Test
  public void onLogoutSuccess_redirectsToSpecifiedUrl_evenIfNoAuthenticationGiven()
      throws Exception {
    handler.onLogoutSuccess(request, response, null);

    verify(response).sendRedirect(EXPECTED_REDIRECT_URL);
  }

}
