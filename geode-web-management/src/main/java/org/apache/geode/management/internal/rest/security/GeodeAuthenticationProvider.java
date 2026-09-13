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

import jakarta.servlet.ServletContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

/**
 * Custom Spring Security AuthenticationProvider that integrates with Geode's SecurityService.
 * Supports both username/password and JWT token authentication modes.
 *
 * <p>
 * <b>Jakarta EE 10 Migration Changes:</b>
 * </p>
 * <ul>
 * <li>javax.servlet.ServletContext → jakarta.servlet.ServletContext (package namespace change)</li>
 * </ul>
 *
 * <p>
 * <b>Authentication Flow:</b>
 * </p>
 * <ol>
 * <li>Receives authentication token from Spring Security filter chain</li>
 * <li>Extracts username and password/token from the authentication object</li>
 * <li>Determines authentication mode:
 * <ul>
 * <li><b>JWT Token Mode:</b> Sets TOKEN property with the JWT token value</li>
 * <li><b>Username/Password Mode:</b> Sets USER_NAME and PASSWORD properties</li>
 * </ul>
 * </li>
 * <li>Delegates to Geode's SecurityService.login() for actual authentication</li>
 * <li>On success: Returns authenticated UsernamePasswordAuthenticationToken</li>
 * <li>On failure: Throws BadCredentialsException (Spring Security standard exception)</li>
 * </ol>
 *
 * <p>
 * <b>Integration with JwtAuthenticationFilter:</b>
 * </p>
 * <ul>
 * <li>JwtAuthenticationFilter extracts JWT token from "Bearer" header</li>
 * <li>Creates UsernamePasswordAuthenticationToken with token as BOTH principal and credentials</li>
 * <li>This provider receives the token in credentials field (password)</li>
 * <li>If authTokenEnabled=true, the credentials value is passed as TOKEN property to
 * SecurityService</li>
 * </ul>
 *
 * <p>
 * <b>ServletContextAware Implementation:</b>
 * </p>
 * <ul>
 * <li>Retrieves SecurityService from servlet context attribute (set by HttpService)</li>
 * <li>Retrieves authTokenEnabled flag from servlet context attribute</li>
 * <li>This allows the provider to be configured dynamically based on Geode's HTTP service
 * settings</li>
 * </ul>
 */
@Component
public class GeodeAuthenticationProvider implements AuthenticationProvider, ServletContextAware {
  private static final Logger logger = LogManager.getLogger();

  private SecurityService securityService;
  private boolean authTokenEnabled;


  public SecurityService getSecurityService() {
    return securityService;
  }

  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {
    Properties credentials = new Properties();
    String username = authentication.getName();
    String password = authentication.getCredentials().toString();

    if (authTokenEnabled) {
      logger.debug("Authenticating with a token.");
      if (password != null) {
        credentials.setProperty(ResourceConstants.TOKEN, password);
      }
    } else {
      logger.debug("Authenticating with a user name and password.");
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
      logger.debug("Authentication was not successful.", e);
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
    logger.debug("Authentication token mode enabled: {}", authTokenEnabled);
  }
}
