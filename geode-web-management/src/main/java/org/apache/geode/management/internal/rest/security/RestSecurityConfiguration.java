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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

import org.apache.geode.management.api.ClusterManagementResult;

/**
 * Spring Security 6.x migration changes:
 *
 * <p>
 * <b>Architecture Changes:</b>
 * </p>
 * <ul>
 * <li>WebSecurityConfigurerAdapter → Component-based configuration (adapter deprecated in Spring
 * Security 5.7, removed in 6.0)</li>
 * <li>Override methods → Bean-based SecurityFilterChain configuration</li>
 * <li>ProviderManager constructor replaces AuthenticationManagerBuilder pattern</li>
 * </ul>
 *
 * <p>
 * <b>API Modernization:</b>
 * </p>
 * <ul>
 * <li>@EnableGlobalMethodSecurity → @EnableMethodSecurity (new annotation name)</li>
 * <li>antMatchers() → requestMatchers() with AntPathRequestMatcher (deprecated method removed)</li>
 * <li>Method chaining (.and()) → Lambda DSL configuration (modern fluent API)</li>
 * <li>authorizeRequests() → authorizeHttpRequests() (new method name)</li>
 * </ul>
 *
 * <p>
 * <b>Multipart Resolver:</b>
 * </p>
 * <ul>
 * <li>CommonsMultipartResolver → StandardServletMultipartResolver</li>
 * <li>Reason: Spring 6.x standardized on Servlet 3.0+ native multipart support</li>
 * <li>Note: Custom isMultipart() logic removed - StandardServletMultipartResolver handles PUT/POST
 * automatically</li>
 * </ul>
 *
 * <p>
 * <b>JWT Authentication Failure Handler:</b>
 * </p>
 * <ul>
 * <li>Added explicit error response handling in authenticationFailureHandler</li>
 * <li>Returns proper HTTP 401 with JSON ClusterManagementResult for UNAUTHENTICATED status</li>
 * <li>Previously relied on default behavior; now explicitly defined for clarity</li>
 * </ul>
 *
 * <p>
 * <b>Security Filter Chain:</b>
 * </p>
 * <ul>
 * <li>configure(HttpSecurity) → filterChain(HttpSecurity) returning SecurityFilterChain</li>
 * <li>SecurityFilterChain bean is Spring Security 6.x's recommended approach</li>
 * <li>setAuthenticationManager() explicitly called on JwtAuthenticationFilter (required in
 * 6.x)</li>
 * </ul>
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
// this package name needs to be different than the admin rest controller's package name
// otherwise this component scan will pick up the admin rest controllers as well.
@ComponentScan("org.apache.geode.management.internal.rest")
public class RestSecurityConfiguration {

  @Autowired
  private GeodeAuthenticationProvider authProvider;

  @Autowired
  private ObjectMapper objectMapper;

  @Bean
  public AuthenticationManager authenticationManager() {
    return new ProviderManager(authProvider);
  }

  @Bean
  public MultipartResolver multipartResolver() {
    // Spring 6.x uses StandardServletMultipartResolver instead of CommonsMultipartResolver
    return new StandardServletMultipartResolver();
  }

  @Bean
  public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
    http.sessionManagement(
        session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(authorize -> authorize
            .requestMatchers(new AntPathRequestMatcher("/docs/**"),
                new AntPathRequestMatcher("/swagger-ui.html"),
                new AntPathRequestMatcher("/swagger-ui/index.html"),
                new AntPathRequestMatcher("/swagger-ui/**"),
                new AntPathRequestMatcher("/"),
                new AntPathRequestMatcher("/v1/api-docs/**"),
                new AntPathRequestMatcher("/webjars/springdoc-openapi-ui/**"),
                new AntPathRequestMatcher("/v3/api-docs/**"),
                new AntPathRequestMatcher("/swagger-resources/**"))
            .permitAll())
        .csrf(csrf -> csrf.disable());

    if (authProvider.getSecurityService().isIntegratedSecurity()) {
      http.authorizeHttpRequests(authorize -> authorize.anyRequest().authenticated());
      // if auth token is enabled, add a filter to parse the request header. The filter still
      // saves the token in the form of UsernamePasswordAuthenticationToken
      if (authProvider.isAuthTokenEnabled()) {
        JwtAuthenticationFilter tokenEndpointFilter = new JwtAuthenticationFilter();
        tokenEndpointFilter.setAuthenticationManager(authenticationManager());
        tokenEndpointFilter.setAuthenticationSuccessHandler((request, response, authentication) -> {
        });
        tokenEndpointFilter.setAuthenticationFailureHandler((request, response, exception) -> {
          try {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            ClusterManagementResult result =
                new ClusterManagementResult(ClusterManagementResult.StatusCode.UNAUTHENTICATED,
                    exception.getMessage());
            objectMapper.writeValue(response.getWriter(), result);
          } catch (IOException e) {
            throw new RuntimeException("Failed to write authentication failure response", e);
          }
        });
        http.addFilterBefore(tokenEndpointFilter, BasicAuthenticationFilter.class);
      }
      http.httpBasic(
          httpBasic -> httpBasic.authenticationEntryPoint(new AuthenticationFailedHandler()));
    } else {
      // When integrated security is disabled, permit all requests
      http.authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll());
    }

    return http.build();
  }

  private class AuthenticationFailedHandler implements AuthenticationEntryPoint {
    private static final String CONTENT_TYPE = MediaType.APPLICATION_JSON_VALUE;

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
        AuthenticationException authException)
        throws IOException, ServletException {
      response.addHeader("WWW-Authenticate", "Basic realm=\"GEODE\"");
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.setContentType(CONTENT_TYPE);
      ClusterManagementResult result =
          new ClusterManagementResult(ClusterManagementResult.StatusCode.UNAUTHENTICATED,
              authException.getMessage());
      objectMapper.writeValue(response.getWriter(), result);
    }
  }
}
