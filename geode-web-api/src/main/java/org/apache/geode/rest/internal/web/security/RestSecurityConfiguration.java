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
package org.apache.geode.rest.internal.web.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;



@Configuration
@EnableWebSecurity
// Spring Security 6.x migration: @EnableGlobalMethodSecurity deprecated, replaced by
// @EnableMethodSecurity
@EnableMethodSecurity(prePostEnabled = true, securedEnabled = true, jsr250Enabled = true)
@ComponentScan("org.apache.geode.rest.internal.web")
public class RestSecurityConfiguration {

  @Autowired
  private GeodeAuthenticationProvider authProvider;

  /**
   * Spring Security 6.x migration: Create AuthenticationManager bean using ProviderManager.
   * Previously configured via AuthenticationManagerBuilder in configure() method.
   */
  @Bean
  public AuthenticationManager authenticationManager() {
    return new ProviderManager(authProvider);
  }

  /**
   * Spring Security 6.x migration: Configure security using SecurityFilterChain bean.
   * Replaces WebSecurityConfigurerAdapter's configure(HttpSecurity) method.
   * Uses lambda-based configuration and authorizeHttpRequests() instead of authorizeRequests().
   */
  @Bean
  public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

    http.sessionManagement(
        session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(authorize -> authorize
            .requestMatchers(new AntPathRequestMatcher("/docs/**"),
                new AntPathRequestMatcher("/swagger-ui.html"),
                new AntPathRequestMatcher("/swagger-ui/index.html"),
                new AntPathRequestMatcher("/swagger-ui/**"),
                new AntPathRequestMatcher("/v1/api-docs/**"),
                new AntPathRequestMatcher("/webjars/springdoc-openapi-ui/**"),
                new AntPathRequestMatcher("/v3/api-docs/**"),
                new AntPathRequestMatcher("/swagger-resources/**"))
            .permitAll())
        .csrf(csrf -> csrf.disable());

    if (authProvider.getSecurityService().isIntegratedSecurity()) {
      http.authorizeHttpRequests(authorize -> authorize.anyRequest().authenticated())
          .httpBasic(httpBasic -> {
          });
    } else {
      // When integrated security is not enabled, permit all other requests
      http.authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll());
    }

    /*
     * CSRF Protection is intentionally disabled for this REST API.
     *
     * JUSTIFICATION:
     *
     * This is a stateless REST API consumed by non-browser clients (CLI tools, SDKs, scripts)
     * using explicit token-based authentication (HTTP Basic Auth). CSRF protection is unnecessary
     * and inappropriate for this use case.
     *
     * WHY CSRF IS NOT NEEDED:
     *
     * 1. STATELESS SESSION POLICY:
     * - Configured with SessionCreationPolicy.STATELESS (see sessionManagement() above)
     * - No HTTP sessions created, no JSESSIONID cookies generated
     * - Server maintains zero session state between requests
     * - Each request is authenticated independently via Authorization header
     *
     * 2. EXPLICIT HEADER-BASED AUTHENTICATION:
     * - Uses HTTP Basic Authentication: Authorization: Basic <base64(username:password)>
     * - Credentials must be explicitly included in HTTP headers on EVERY request
     * - Browsers do NOT automatically send Authorization headers (unlike cookies)
     * - Clients must programmatically set headers for each API call
     * - See GeodeDevRestClient.doRequest() for reference implementation
     *
     * 3. NO AUTOMATIC CREDENTIAL TRANSMISSION:
     * - CSRF attacks exploit browsers' automatic cookie submission to authenticated domains
     * - Authorization headers require explicit JavaScript code to set (not automatic)
     * - Same-Origin Policy (SOP) blocks cross-origin header access without CORS consent
     * - Even if attacker hosts malicious page, cannot extract or send Authorization header
     *
     * 4. NON-BROWSER CLIENT ARCHITECTURE:
     * - Primary consumers: gfsh CLI, Java/Python SDKs, curl scripts, automation tools
     * - These clients don't execute arbitrary JavaScript from untrusted sources
     * - No risk of user visiting malicious website while authenticated
     * - Browser-based consumption would violate API's stateless design contract
     *
     * 5. CORS PROTECTION LAYER:
     * - Cross-Origin Resource Sharing (CORS) provides boundary protection
     * - Browsers enforce preflight OPTIONS requests for cross-origin API calls
     * - Custom Authorization headers trigger CORS preflight checks
     * - Server must explicitly whitelist origins via Access-Control-Allow-Origin
     * - Default CORS policy blocks unauthorized cross-origin requests
     *
     * 6. SPRING SECURITY RECOMMENDATIONS:
     * Official Spring Security documentation states:
     * "If you are only creating a service that is used by non-browser clients,
     * you will likely want to disable CSRF protection."
     * Source: https://docs.spring.io/spring-security/reference/servlet/exploits/csrf.html
     *
     * WHEN CSRF WOULD BE REQUIRED:
     *
     * - Browser-based UI with session cookies (see geode-pulse for contrast)
     * - Form-based authentication with automatic cookie submission
     * - SessionCreationPolicy.IF_REQUIRED or ALWAYS
     * - State-changing operations via GET requests (poor REST design)
     * - Cookie-based authentication without additional token validation
     *
     * SECURITY MEASURES IN PLACE:
     *
     * - Authentication required on every request (no persistent sessions)
     * - Authorization via @PreAuthorize annotations on endpoints
     * - HTTPS/TLS encryption required in production (protects credentials in transit)
     * - Credentials stored in CredentialsProvider, not in browser cookie storage
     * - No state stored on server between requests (eliminates session hijacking)
     *
     * ALTERNATIVE CONSIDERED:
     *
     * Enabling CSRF with CookieCsrfTokenRepository would be inappropriate because:
     * - Adds unnecessary complexity for stateless API clients
     * - Requires clients to perform extra GET request to obtain CSRF token
     * - Violates REST statelessness principle (server-side token storage)
     * - Provides no security benefit (no cookies to protect against CSRF)
     * - Breaks compatibility with standard REST client libraries
     *
     * VERIFICATION:
     *
     * See test evidence in:
     * - GeodeDevRestClient: Demonstrates per-request Basic Auth without sessions
     * - RestFunctionExecuteDUnitTest: Shows explicit credentials on each API call
     * - No login endpoint exists (contrast with Pulse's /login.html)
     * - No session cookie handling in client code
     *
     * CONCLUSION:
     *
     * CSRF protection is disabled by design for this stateless REST API. This configuration
     * aligns with Spring Security best practices, industry standards for REST APIs, and the
     * architectural requirements of Geode's programmatic client ecosystem.
     *
     * Last reviewed: Jakarta EE 10 migration (2024)
     * Related: geode-pulse uses DIFFERENT security model (browser-based, session cookies)
     */

    return http.build();
  }
}
