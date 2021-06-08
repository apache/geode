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

import java.util.Properties;

import javax.servlet.ServletContext;

import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.GemFireSecurityException;


@Component
public class GeodeAuthenticationProvider implements AuthenticationProvider, ServletContextAware {

  private SecurityService securityService;
  private boolean authTokenEnabled;


  public SecurityService getSecurityService() {
    return this.securityService;
  }

  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {
    Properties credentials = new Properties();
    String username = authentication.getName();
    String password = authentication.getCredentials().toString();

    if (authTokenEnabled) {
      if (password != null) {
        credentials.setProperty(ResourceConstants.TOKEN, password);
      }
    } else {
      if (username != null) {
        credentials.put(ResourceConstants.USER_NAME, username);
      }
      if (password != null) {
        credentials.put(ResourceConstants.PASSWORD, password);
      }
    }

    try {
      securityService.login(credentials);
      return new UsernamePasswordAuthenticationToken(username, password,
          AuthorityUtils.NO_AUTHORITIES);
    } catch (GemFireSecurityException e) {
      throw new BadCredentialsException(e.getLocalizedMessage(), e);
    }
  }

  @Override
  public boolean supports(Class<?> authentication) {
    return authentication.isAssignableFrom(UsernamePasswordAuthenticationToken.class);
  }

  public boolean isAuthTokenEnabled() {
    return authTokenEnabled;
  }

  @Override
  public void setServletContext(ServletContext servletContext) {
    securityService = (SecurityService) servletContext
        .getAttribute(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM);
    authTokenEnabled =
        (Boolean) servletContext.getAttribute(HttpService.AUTH_TOKEN_ENABLED_PARAM);
  }
}
