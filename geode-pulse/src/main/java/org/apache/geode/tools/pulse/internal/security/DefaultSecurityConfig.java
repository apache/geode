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
        .csrf(csrf -> csrf.disable());
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
