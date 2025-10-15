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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.client.oidc.web.logout.OidcClientInitiatedLogoutSuccessHandler;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

/**
 * Spring Security 6.x Migration:
 * - Changed from extending WebSecurityConfigurerAdapter to using SecurityFilterChain bean
 * - Changed @EnableGlobalMethodSecurity to @EnableMethodSecurity
 * - Changed authorizeRequests() to authorizeHttpRequests()
 * - Changed mvcMatchers() to requestMatchers() with AntPathRequestMatcher
 * - WebSecurityConfigurerAdapter is removed in Spring Security 6.x
 *
 * CSRF Protection for OAuth2 Web Applications:
 *
 * This configuration enables CSRF protection for Pulse when using OAuth2 authentication.
 * CSRF protection is REQUIRED for OAuth2 flows because:
 *
 * 1. SESSION-BASED AUTHENTICATION:
 * - OAuth2 login establishes server-side sessions with session cookies
 * - Session cookies are automatically transmitted by browsers
 * - This creates vulnerability to Cross-Site Request Forgery attacks
 *
 * 2. CSRF ATTACK VECTOR IN OAUTH2:
 * - Malicious site can forge requests using user's authenticated session
 * - Browser automatically includes session cookies in cross-origin requests
 * - Without CSRF tokens, server cannot distinguish legitimate from forged requests
 * - Example: Malicious site submits form to /pulse/admin-action using victim's session
 *
 * 3. SPRING SECURITY CSRF IMPLEMENTATION:
 * - Default configuration uses session-based CSRF token storage
 * - Tokens are validated on state-changing requests (POST, PUT, DELETE, PATCH)
 * - GET requests are exempt from CSRF validation (safe methods)
 * - OAuth2 authorization flow includes 'state' parameter for CSRF protection
 *
 * 4. OAUTH2 SPECIFICATION COMPLIANCE:
 * - RFC 6749 Section 10.12 recommends CSRF protection for OAuth2 clients
 * - OAuth2 'state' parameter provides CSRF protection during authorization flow
 * - Session-based applications require additional CSRF protection for post-login requests
 *
 * 5. SECURITY BEST PRACTICES:
 * - OWASP recommends CSRF protection for all session-based web applications
 * - Defense-in-depth: Multiple layers of protection against CSRF attacks
 * - Complements OAuth2 state parameter with application-level CSRF tokens
 *
 * IMPLEMENTATION DETAILS:
 *
 * - Uses Spring Security's default CSRF configuration
 * - CSRF tokens stored in HTTP session (server-side)
 * - Client must include CSRF token in requests via:
 * * _csrf parameter in forms
 * * X-CSRF-TOKEN header in AJAX requests
 * - Tokens automatically generated and validated by Spring Security
 *
 * IMPACT ON CLIENT APPLICATIONS:
 *
 * - JavaScript applications must obtain and include CSRF tokens
 * - Forms must include hidden _csrf field (automatically added by Spring forms)
 * - AJAX requests must include X-CSRF-TOKEN header
 * - Modern SPA frameworks can obtain token from meta tag or endpoint
 *
 * SECURITY BENEFITS:
 *
 * - Prevents unauthorized actions via forged requests
 * - Protects OAuth2-authenticated users from CSRF attacks
 * - Maintains security even if other defenses (CORS, SameSite) are bypassed
 * - Complies with security scanning tools (CodeQL, OWASP ZAP)
 *
 * REFERENCES:
 *
 * - OWASP CSRF Prevention:
 * https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html
 * - RFC 6749 OAuth2 Security: https://tools.ietf.org/html/rfc6749#section-10.12
 * - Spring Security CSRF:
 * https://docs.spring.io/spring-security/reference/servlet/exploits/csrf.html
 * - CodeQL Rule java/spring-disabled-csrf-protection
 *
 * Last updated: Jakarta EE 10 migration (October 2024)
 * Security review: CSRF protection enabled for OAuth2 session-based authentication
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
@Profile("pulse.authentication.oauth")
public class OAuthSecurityConfig {
  private final LogoutHandler repositoryLogoutHandler;
  private final LogoutSuccessHandler oidcLogoutHandler;

  @Autowired
  public OAuthSecurityConfig(RepositoryLogoutHandler repositoryLogoutHandler,
      OidcClientInitiatedLogoutSuccessHandler oidcLogoutHandler) {
    this.oidcLogoutHandler = oidcLogoutHandler;
    this.repositoryLogoutHandler = repositoryLogoutHandler;
  }

  @Bean
  public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
    http.authorizeHttpRequests(authorize -> authorize
        .requestMatchers(new AntPathRequestMatcher("/pulseVersion"),
            new AntPathRequestMatcher("/scripts/**"),
            new AntPathRequestMatcher("/images/**"),
            new AntPathRequestMatcher("/css/**"),
            new AntPathRequestMatcher("/properties/**"))
        .permitAll()
        .requestMatchers(new AntPathRequestMatcher("/dataBrowser*"),
            new AntPathRequestMatcher("/getQueryStatisticsGridModel*"))
        .access((authentication,
            context) -> new org.springframework.security.authorization.AuthorizationDecision(
                authentication.get().getAuthorities().stream()
                    .anyMatch(a -> a.getAuthority().equals("SCOPE_CLUSTER:READ"))
                    && authentication.get().getAuthorities().stream()
                        .anyMatch(a -> a.getAuthority().equals("SCOPE_DATA:READ"))))
        .requestMatchers(new AntPathRequestMatcher("/*"))
        .hasAuthority("SCOPE_CLUSTER:READ")
        .anyRequest().authenticated())
        .oauth2Login(oauth -> oauth.defaultSuccessUrl("/clusterDetail.html", true))
        .exceptionHandling(exception -> exception.accessDeniedPage("/accessDenied.html"))
        .logout(logout -> logout
            .logoutUrl("/clusterLogout")
            .addLogoutHandler(repositoryLogoutHandler)
            .logoutSuccessHandler(oidcLogoutHandler))
        .headers(header -> header
            .frameOptions(frameOptions -> frameOptions.deny())
            .xssProtection(xss -> xss.headerValue(
                org.springframework.security.web.header.writers.XXssProtectionHeaderWriter.HeaderValue.ENABLED_MODE_BLOCK))
            .contentTypeOptions(contentTypeOptions -> {
            }));

    // CSRF Protection: ENABLED for OAuth2 session-based authentication
    //
    // Technical rationale:
    // - OAuth2 login creates server-side sessions with automatic cookie transmission
    // - Browsers include session cookies in cross-origin requests (CSRF attack vector)
    // - Spring Security's default CSRF protection uses session-based token storage
    // - Tokens validated on state-changing HTTP methods (POST, PUT, DELETE, PATCH)
    // - Complements OAuth2 'state' parameter with application-level CSRF protection
    //
    // Client integration:
    // - Forms: Include _csrf parameter (Spring forms add automatically)
    // - AJAX: Include X-CSRF-TOKEN header (obtain from meta tag or /csrf endpoint)
    // - SPA: Modern frameworks support CSRF token integration
    //
    // Security compliance:
    // - Fixes CodeQL vulnerability: java/spring-disabled-csrf-protection
    // - Follows OWASP CSRF prevention recommendations
    // - Aligns with RFC 6749 OAuth2 security considerations
    // - Default configuration sufficient for most OAuth2 web applications
    return http.build();
  }
}
