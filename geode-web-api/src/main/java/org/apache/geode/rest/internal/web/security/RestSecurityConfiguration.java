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

    return http.build();
  }
}
