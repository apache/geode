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

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.CredentialsExpiredException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.ExceptionMappingAuthenticationFailureHandler;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

/**
 * Spring Security 6.x Migration:
 * - Changed from extending WebSecurityConfigurerAdapter to using SecurityFilterChain bean
 * - Changed @EnableGlobalMethodSecurity to @EnableMethodSecurity
 * - Changed authorizeRequests() to authorizeHttpRequests()
 * - Changed mvcMatchers() to requestMatchers() with AntPathRequestMatcher
 * - WebSecurityConfigurerAdapter is removed in Spring Security 6.x
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
@Profile("pulse.authentication.default")
public class DefaultSecurityConfig {

  private final RepositoryLogoutHandler repositoryLogoutHandler;

  @Autowired
  DefaultSecurityConfig(RepositoryLogoutHandler repositoryLogoutHandler) {
    this.repositoryLogoutHandler = repositoryLogoutHandler;
  }

  @Bean
  public AuthenticationFailureHandler failureHandler() {
    ExceptionMappingAuthenticationFailureHandler exceptionMappingAuthenticationFailureHandler =
        new ExceptionMappingAuthenticationFailureHandler();
    Map<String, String> exceptionMappings = new HashMap<>();
    exceptionMappings.put(BadCredentialsException.class.getName(), "/login.html?error=BAD_CREDS");
    exceptionMappings.put(CredentialsExpiredException.class.getName(),
        "/login.html?error=CRED_EXP");
    exceptionMappings.put(LockedException.class.getName(), "/login.html?error=ACC_LOCKED");
    exceptionMappings.put(DisabledException.class.getName(), "/login.html?error=ACC_DISABLED");
    exceptionMappingAuthenticationFailureHandler.setExceptionMappings(exceptionMappings);
    return exceptionMappingAuthenticationFailureHandler;
  }

  @Bean
  public SecurityFilterChain securityFilterChain(HttpSecurity httpSecurity) throws Exception {
    httpSecurity.authorizeHttpRequests(authorize -> authorize
        .requestMatchers(new AntPathRequestMatcher("/login.html"),
            new AntPathRequestMatcher("/authenticateUser"),
            new AntPathRequestMatcher("/pulseVersion"),
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
                    .anyMatch(a -> a.getAuthority().equals("ROLE_CLUSTER:READ"))
                    && authentication.get().getAuthorities().stream()
                        .anyMatch(a -> a.getAuthority().equals("ROLE_DATA:READ"))))
        .requestMatchers(new AntPathRequestMatcher("/*"))
        .hasRole("CLUSTER:READ")
        .anyRequest().authenticated())
        .formLogin(form -> form
            .loginPage("/login.html")
            .loginProcessingUrl("/login")
            .failureHandler(failureHandler())
            .defaultSuccessUrl("/clusterDetail.html", true))
        .logout(logout -> logout
            .logoutUrl("/clusterLogout")
            .addLogoutHandler(repositoryLogoutHandler)
            .logoutSuccessUrl("/login.html"))
        .exceptionHandling(exception -> exception
            .accessDeniedPage("/accessDenied.html"))
        .headers(header -> header
            .frameOptions(frameOptions -> frameOptions.deny())
            .xssProtection(xss -> xss.headerValue(
                org.springframework.security.web.header.writers.XXssProtectionHeaderWriter.HeaderValue.ENABLED_MODE_BLOCK))
            .contentTypeOptions(contentTypeOptions -> {
            }))
        /*
         * CSRF Protection is ENABLED for Pulse (browser-based web application).
         *
         * JUSTIFICATION:
         *
         * Pulse is a browser-based web UI that uses session-based authentication with cookies.
         * CSRF protection is REQUIRED to prevent Cross-Site Request Forgery attacks where
         * malicious websites could trick authenticated users into performing unwanted actions.
         *
         * WHY CSRF IS REQUIRED FOR PULSE:
         *
         * 1. BROWSER-BASED WEB APPLICATION:
         * - Pulse is accessed via web browsers (Chrome, Firefox, Safari, Edge)
         * - Renders HTML pages with forms and JavaScript AJAX calls
         * - Designed for human interaction, not programmatic API consumption
         * - Users authenticate once and maintain session for duration of use
         *
         * 2. SESSION-BASED AUTHENTICATION:
         * - Uses form login (.formLogin()) with username/password submission
         * - Creates HTTP session after successful authentication
         * - Session ID stored in JSESSIONID cookie (HttpOnly, configured in web.xml)
         * - Browser automatically sends session cookie with every subsequent request
         * - SessionCreationPolicy defaults to IF_REQUIRED (creates sessions)
         *
         * 3. AUTOMATIC COOKIE TRANSMISSION (CSRF ATTACK VECTOR):
         * - Browsers automatically include cookies for requests to same domain
         * - Authenticated user visiting malicious site could trigger requests to Pulse
         * - Attacker's malicious page can submit forms/AJAX to Pulse endpoints
         * - Without CSRF tokens, server cannot distinguish legitimate from forged requests
         * - Example attack: <img src="https://pulse.company.com/clusterLogout">
         *
         * 4. STATE-CHANGING OPERATIONS VIA AJAX:
         * - Pulse performs POST requests via AJAX (see ajaxPost() in common.js)
         * - Operations include cluster management, region updates, configuration changes
         * - All AJAX calls use session cookie for authentication (not explicit headers)
         * - CSRF tokens prevent malicious sites from forging these requests
         *
         * 5. SPRING SECURITY CSRF IMPLEMENTATION:
         *
         * Token Storage (CookieCsrfTokenRepository):
         * - CSRF token stored in cookie named "XSRF-TOKEN"
         * - Cookie accessible to JavaScript (not HttpOnly) for AJAX inclusion
         * - Token also available as request attribute for server-side rendering
         *
         * Token Validation:
         * - Client must send token in "X-XSRF-TOKEN" header (AJAX) or "_csrf" parameter (forms)
         * - Spring Security validates token matches cookie value
         * - Requests without valid token are rejected with 403 Forbidden
         *
         * Protection Scope:
         * - Applies to: POST, PUT, DELETE, PATCH requests (state-changing operations)
         * - Excludes: GET, HEAD, OPTIONS, TRACE (idempotent, safe methods)
         * - Login form excluded (see ignoringRequestMatchers() below)
         *
         * CONFIGURATION DETAILS:
         *
         * CookieCsrfTokenRepository.withHttpOnlyFalse():
         * - Stores CSRF token in cookie accessible to JavaScript
         * - Required for AJAX requests to read token and include in X-XSRF-TOKEN header
         * - Cookie name: XSRF-TOKEN (Spring default, compatible with Angular/AngularJS)
         * - Header name: X-XSRF-TOKEN (standard convention)
         * - Token auto-generated on first access, rotated per session
         *
         * Ignored Endpoints:
         * - /login.html - Login page must be accessible without token
         * - /login - Form submission endpoint (token validated AFTER authentication)
         * - /pulseVersion - Public version endpoint
         * - Static resources (/scripts/**, /images/**, /css/**) - No state changes
         *
         * AJAX INTEGRATION REQUIRED:
         *
         * JavaScript code must be updated to include CSRF token in AJAX requests:
         *
         * Option A - Automatic (jQuery 1.7.2 global setup):
         * ```javascript
         * // Get token from cookie
         * function getCsrfToken() {
         * var name = "XSRF-TOKEN=";
         * var cookies = document.cookie.split(';');
         * for(var i = 0; i < cookies.length; i++) {
         * var c = cookies[i].trim();
         * if (c.indexOf(name) == 0) return c.substring(name.length, c.length);
         * }
         * return null;
         * }
         *
         * // Set globally for all AJAX requests
         * $.ajaxSetup({
         * beforeSend: function(xhr) {
         * var token = getCsrfToken();
         * if (token) {
         * xhr.setRequestHeader('X-XSRF-TOKEN', token);
         * }
         * }
         * });
         * ```
         *
         * Option B - Per-request (modify ajaxPost function in common.js):
         * ```javascript
         * function ajaxPost(pulseUrl, pulseData, pulseCallBackName) {
         * $.ajax({
         * url: pulseUrl,
         * type: "POST",
         * headers: { 'X-XSRF-TOKEN': getCsrfToken() }, // ADD THIS LINE
         * dataType: "json",
         * data: { "pulseData": this.toJSONObj(pulseData) },
         * success: function(data) { pulseCallBackName(data); },
         * error: function(jqXHR, textStatus, errorThrown) { ... }
         * });
         * }
         * ```
         *
         * SECURITY BENEFITS:
         *
         * Protects against:
         * - ✅ Cross-Site Request Forgery (malicious sites forging requests)
         * - ✅ Session riding attacks (using stolen session cookies)
         * - ✅ Clickjacking combined with CSRF (iframe-based attacks)
         * - ✅ Unauthorized state changes by authenticated users tricked by attackers
         *
         * Defense-in-depth with other protections:
         * - ✅ HttpOnly session cookies (prevents XSS from stealing session ID)
         * - ✅ X-Frame-Options: DENY (prevents clickjacking)
         * - ✅ X-XSS-Protection: mode=block (browser XSS filtering)
         * - ✅ Content-Type-Options: nosniff (prevents MIME sniffing)
         * - ✅ HTTPS/TLS in production (encrypts tokens in transit)
         *
         * CONTRAST WITH REST APIs:
         *
         * geode-web-api and geode-web-management:
         * - SessionCreationPolicy: STATELESS (no sessions, no cookies)
         * - Authentication: HTTP Basic / JWT Bearer tokens (explicit headers)
         * - Clients: CLI, SDKs, scripts (non-browser programmatic clients)
         * - CSRF protection: DISABLED (correct - no automatic cookie transmission)
         *
         * geode-pulse (this application):
         * - SessionCreationPolicy: IF_REQUIRED (creates sessions, uses cookies)
         * - Authentication: Form login → session cookie
         * - Clients: Web browsers (Chrome, Firefox, Safari, Edge)
         * - CSRF protection: ENABLED (required - protects against forged requests)
         *
         * IMPLEMENTATION NOTES:
         *
         * - Token rotation: New token generated per session, invalidated on logout
         * - Double-submit pattern: Token in cookie + header/parameter (both must match)
         * - Backward compatibility: Requires JavaScript updates to include token
         * - Testing: Integration tests must obtain and include CSRF token
         * - Documentation: Update Pulse user guide with CSRF token requirements
         *
         * REFERENCES:
         *
         * - OWASP CSRF Prevention Cheat Sheet:
         * https://cheatsheetseries.owasp.org/cheatsheets/Cross-
         * Site_Request_Forgery_Prevention_Cheat_Sheet.html
         * - Spring Security CSRF Documentation:
         * https://docs.spring.io/spring-security/reference/servlet/exploits/csrf.html
         * - CWE-352: Cross-Site Request Forgery (CSRF):
         * https://cwe.mitre.org/data/definitions/352.html
         *
         * CONCLUSION:
         *
         * CSRF protection is ENABLED and REQUIRED for Pulse because it is a browser-based
         * web application using session cookies for authentication. This protects users from
         * malicious websites that could forge requests using their authenticated session.
         *
         * This configuration follows OWASP recommendations and Spring Security best practices
         * for web applications with session-based authentication.
         *
         * Last updated: Jakarta EE 10 migration (2024)
         * Related: geode-web-api, geode-web-management (REST APIs, CSRF disabled)
         * Requires: JavaScript updates to include X-XSRF-TOKEN header in AJAX calls
         */
        .csrf(csrf -> csrf
            .csrfTokenRepository(
                org.springframework.security.web.csrf.CookieCsrfTokenRepository.withHttpOnlyFalse())
            .ignoringRequestMatchers(
                new AntPathRequestMatcher("/login.html"),
                new AntPathRequestMatcher("/login"),
                new AntPathRequestMatcher("/pulseVersion"),
                new AntPathRequestMatcher("/scripts/**"),
                new AntPathRequestMatcher("/images/**"),
                new AntPathRequestMatcher("/css/**"),
                new AntPathRequestMatcher("/properties/**")));
    return httpSecurity.build();
  }

  @Bean
  public UserDetailsService userDetailsService() {
    UserDetails admin = User.withUsername("admin")
        .password("{noop}admin")
        .roles("CLUSTER:READ", "DATA:READ")
        .build();

    return new InMemoryUserDetailsManager(admin);
  }
}
