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
            }))
        .csrf(csrf -> csrf.disable());
    return http.build();
  }
}
