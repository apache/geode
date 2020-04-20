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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.security.core.Authentication;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.tools.pulse.internal.data.Repository;

@Category({PulseTest.class, SecurityTest.class, LoggingTest.class})
public class RepositoryLogoutHandlerTest {
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private Repository repository;

  private RepositoryLogoutHandler handler;

  @Before
  public void setup() {
    handler = new RepositoryLogoutHandler(repository);
  }

  @Test
  public void logsOutAuthenticatedUser() {
    String authenticatedUser = "authenticated-user";

    Authentication authentication = mock(Authentication.class);
    when(authentication.getName()).thenReturn(authenticatedUser);

    handler.logout(null, null, authentication);

    verify(repository, times(1)).logoutUser(authenticatedUser);
  }
}
