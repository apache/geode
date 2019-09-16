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

package org.apache.geode.management.internal.rest.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;

public class JwtAuthenticationFilterTest {

  private JwtAuthenticationFilter filter;
  private HttpServletRequest request;

  @Before
  public void before() throws Exception {
    filter = new JwtAuthenticationFilter();
    request = mock(HttpServletRequest.class);
  }

  @Test
  public void nullHeader() throws Exception {
    when(request.getHeader("Authorization")).thenReturn(null);
    assertThatThrownBy(() -> filter.attemptAuthentication(request, null))
        .isInstanceOf(BadCredentialsException.class);
  }

  @Test
  public void notBearer() throws Exception {
    when(request.getHeader("Authorization")).thenReturn("foo bar");
    assertThatThrownBy(() -> filter.attemptAuthentication(request, null))
        .isInstanceOf(BadCredentialsException.class);
  }

  @Test
  public void wrongFormat() throws Exception {
    when(request.getHeader("Authorization")).thenReturn("foo bar foo");
    assertThatThrownBy(() -> filter.attemptAuthentication(request, null))
        .isInstanceOf(BadCredentialsException.class);
  }

  @Test
  public void correctHeader() throws Exception {
    when(request.getHeader("Authorization")).thenReturn("Bearer bar");
    Authentication authentication = filter.attemptAuthentication(request, null);
    assertThat(authentication.getPrincipal().toString()).isEqualTo("Bearer");
    assertThat(authentication.getCredentials().toString()).isEqualTo("bar");
  }
}
