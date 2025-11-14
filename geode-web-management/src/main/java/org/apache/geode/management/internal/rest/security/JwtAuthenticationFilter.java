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

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;

/**
 * Json Web Token authentication filter. This would filter the requests with "Bearer" token in the
 * authentication header, and put the token in the form of UsernamePasswordAuthenticationToken
 * format for the downstream to consume.
 *
 * <p>
 * <b>Jakarta EE 10 Migration Changes:</b>
 * </p>
 * <ul>
 * <li>javax.servlet.* → jakarta.servlet.* (package namespace change)</li>
 * </ul>
 *
 * <p>
 * <b>Spring Security 6.x Migration - Critical Bug Fixes:</b>
 * </p>
 * <ul>
 * <li><b>requiresAuthentication() Fix:</b> Changed from always returning {@code true} to properly
 * checking for "Bearer " token presence. Previously processed ALL requests; now only processes
 * requests with JWT tokens, avoiding unnecessary authentication attempts.</li>
 *
 * <li><b>Token Parsing Fix:</b> Changed {@code split(" ")} to {@code split(" ", 2)} to handle
 * tokens
 * containing spaces correctly. Without limit parameter, tokens with embedded spaces would be
 * incorrectly split into multiple parts.</li>
 *
 * <li><b>Token Placement Fix:</b> Fixed critical bug where "Bearer" string was passed as username
 * and token as password. Now correctly passes token as BOTH principal and credentials (tokens[1],
 * tokens[1]).
 * GeodeAuthenticationProvider expects the JWT token in the credentials field.</li>
 *
 * <li><b>Authentication Execution Fix:</b> Added explicit call to
 * {@code getAuthenticationManager().authenticate()}
 * to actually validate the token. Previously, attemptAuthentication() returned an unauthenticated
 * token,
 * bypassing actual authentication. Spring Security 6.x requires filters to return authenticated
 * tokens.</li>
 *
 * <li><b>Error Handling Enhancement:</b> Added {@code unsuccessfulAuthentication()} override to
 * properly
 * log authentication failures. This helps diagnose JWT authentication issues in production.</li>
 * </ul>
 *
 * <p>
 * <b>Debug Logging:</b>
 * </p>
 * <ul>
 * <li>Added comprehensive logging throughout authentication flow for troubleshooting</li>
 * <li>Logs: filter initialization, authentication requirements check, token parsing, authentication
 * attempts, success/failure outcomes</li>
 * </ul>
 */
public class JwtAuthenticationFilter extends AbstractAuthenticationProcessingFilter {
  private static final Logger logger = LogManager.getLogger();

  public JwtAuthenticationFilter() {
    super("/**");
    logger.info("JwtAuthenticationFilter initialized");
  }

  @Override
  protected boolean requiresAuthentication(HttpServletRequest request,
      HttpServletResponse response) {
    String header = request.getHeader("Authorization");
    boolean requires = header != null && header.startsWith("Bearer ");
    logger.info("requiresAuthentication() - URI: {}, Authorization header: {}, requires: {}",
        request.getRequestURI(), header, requires);
    return requires;
  }

  @Override
  public Authentication attemptAuthentication(HttpServletRequest request,
      HttpServletResponse response) throws AuthenticationException {

    String header = request.getHeader("Authorization");
    logger.info("attemptAuthentication() - URI: {}, Authorization header: {}",
        request.getRequestURI(), header);

    if (header == null || !header.startsWith("Bearer ")) {
      logger.error("No JWT token found - header: {}", header);
      throw new BadCredentialsException("No JWT token found in request headers, header: " + header);
    }

    String[] tokens = header.split(" ", 2);
    logger.info("Split token - length: {}, token[0]: {}, token[1]: {}",
        tokens.length, tokens[0], tokens.length > 1 ? tokens[1] : "N/A");

    if (tokens.length != 2) {
      logger.error("Wrong authentication header format: {}", header);
      throw new BadCredentialsException("Wrong authentication header format: " + header);
    }

    // FIX: Pass the token as credentials (password), not "Bearer" as username
    // GeodeAuthenticationProvider expects the token in the credentials/password field
    UsernamePasswordAuthenticationToken authToken =
        new UsernamePasswordAuthenticationToken(tokens[1], tokens[1]);
    logger.info("Created UsernamePasswordAuthenticationToken - principal: {}, credentials: {}",
        authToken.getPrincipal(), authToken.getCredentials());

    // CRITICAL: Call AuthenticationManager to actually authenticate the token
    // AbstractAuthenticationProcessingFilter expects us to return an authenticated token
    logger.info("Calling getAuthenticationManager().authenticate()");
    return getAuthenticationManager().authenticate(authToken);
  }

  @Override
  protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response,
      FilterChain chain, Authentication authResult)
      throws IOException, ServletException {
    logger.info("successfulAuthentication() - authResult: {}, principal: {}",
        authResult, authResult != null ? authResult.getPrincipal() : "null");
    super.successfulAuthentication(request, response, chain, authResult);

    // As this authentication is in HTTP header, after success we need to continue the request
    // normally and return the response as if the resource was not secured at all
    chain.doFilter(request, response);
  }

  @Override
  protected void unsuccessfulAuthentication(HttpServletRequest request,
      HttpServletResponse response, AuthenticationException failed)
      throws IOException, ServletException {
    logger.error("unsuccessfulAuthentication() - URI: {}, exception: {}",
        request.getRequestURI(), failed.getMessage(), failed);
    super.unsuccessfulAuthentication(request, response, failed);
  }
}
