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
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
@Profile("pulse.authentication.gemfire")
public class GemfireSecurityConfig extends DefaultSecurityConfig {
  private final AuthenticationProvider authenticationProvider;

  @Autowired
  public GemfireSecurityConfig(GemFireAuthenticationProvider gemFireAuthenticationProvider,
      RepositoryLogoutHandler repositoryLogoutHandler) {
    super(repositoryLogoutHandler);
    authenticationProvider = gemFireAuthenticationProvider;
  }

  @Bean
  @Override
  public SecurityFilterChain securityFilterChain(HttpSecurity httpSecurity) throws Exception {
    // Spring Security 6 migration: Explicitly register custom AuthenticationProvider
    // to ensure GemFire authentication is used instead of the default UserDetailsService
    httpSecurity.authenticationProvider(authenticationProvider);
    return super.securityFilterChain(httpSecurity);
  }

  // Spring Security 6 migration: Provide AuthenticationManager bean for custom authentication
  // This enables form login to use GemFireAuthenticationProvider for user authentication
  @Bean
  public AuthenticationManager authenticationManager() {
    return new ProviderManager(authenticationProvider);
  }
}
