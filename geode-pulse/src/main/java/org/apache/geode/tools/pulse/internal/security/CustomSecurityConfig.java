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
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;

/**
 * Configures Pulse to use the authentication manager defined by the
 * {@code pulse-authentication-custom.xml} file, which <em>must</em> define an authentication
 * manager. This configuration is applied when the {@code pulse.authentication.custom} profile is
 * active.
 */
@Configuration
@EnableWebSecurity
// @EnableGlobalMethodSecurity deprecated in Spring Security 6.x, replaced by @EnableMethodSecurity
@EnableMethodSecurity(prePostEnabled = true)
@Profile("pulse.authentication.custom")
@ImportResource("classpath:pulse-authentication-custom.xml")
public class CustomSecurityConfig extends DefaultSecurityConfig {
  private final AuthenticationManager customAuthenticationManager;

  @Autowired
  CustomSecurityConfig(AuthenticationManager authenticationManager,
      RepositoryLogoutHandler repositoryLogoutHandler) {
    super(repositoryLogoutHandler);
    this.customAuthenticationManager = authenticationManager;
  }

  /**
   * Custom authentication uses an externally defined AuthenticationManager, not UserDetailsService.
   * Override parent's UserDetailsService bean to prevent it from being used.
   */
  @Bean
  @Override
  public UserDetailsService userDetailsService() {
    // Return a dummy UserDetailsService that throws an exception if used
    // This should not be used since we have a custom AuthenticationManager
    return username -> {
      throw new UnsupportedOperationException(
          "UserDetailsService should not be used with custom authentication");
    };
  }

  /**
   * Expose the custom AuthenticationManager as a bean.
   * Spring Security 6.x removed configure(AuthenticationManagerBuilder), requiring direct bean
   * definition.
   */
  @Bean
  public AuthenticationManager authenticationManager() {
    return customAuthenticationManager;
  }
}
