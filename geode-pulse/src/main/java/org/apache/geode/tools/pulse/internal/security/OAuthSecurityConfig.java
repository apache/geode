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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.client.oidc.web.logout.OidcClientInitiatedLogoutSuccessHandler;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@Profile("pulse.authentication.oauth")
public class OAuthSecurityConfig extends WebSecurityConfigurerAdapter {
  private final LogoutHandler repositoryLogoutHandler;
  private final LogoutSuccessHandler oidcLogoutHandler;

  @Autowired
  public OAuthSecurityConfig(RepositoryLogoutHandler repositoryLogoutHandler,
      OidcClientInitiatedLogoutSuccessHandler oidcLogoutHandler) {
    this.oidcLogoutHandler = oidcLogoutHandler;
    this.repositoryLogoutHandler = repositoryLogoutHandler;
  }

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.authorizeRequests(authorize -> authorize
        .mvcMatchers("/pulseVersion", "/scripts/**", "/images/**", "/css/**", "/properties/**")
        .permitAll()
        .mvcMatchers("/dataBrowser*", "/getQueryStatisticsGridModel*")
        .access("hasAuthority('SCOPE_CLUSTER:READ') and hasAuthority('SCOPE_DATA:READ')")
        .mvcMatchers("/*")
        .hasAuthority("SCOPE_CLUSTER:READ")
        .anyRequest().authenticated())
        .oauth2Login(oauth -> oauth.defaultSuccessUrl("/clusterDetail.html", true))
        .exceptionHandling(exception -> exception.accessDeniedPage("/accessDenied.html"))
        .logout(logout -> logout
            .logoutUrl("/clusterLogout")
            .addLogoutHandler(repositoryLogoutHandler)
            .logoutSuccessHandler(oidcLogoutHandler))
        .headers(header -> header
            .frameOptions().deny()
            .xssProtection(xss -> xss
                .xssProtectionEnabled(true)
                .block(true))
            .contentTypeOptions())
        .csrf().disable();
  }
}
