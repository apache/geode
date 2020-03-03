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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.ExceptionMappingAuthenticationFailureHandler;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
// @ComponentScan("org.apache.geode.tools.pulse.internal")
public class PulseSecurityConfig {

  public GemFireAuthenticationProvider gemAuthenticationProvider() {
    return new GemFireAuthenticationProvider();
  }

  public LogoutHandler customLogoutSuccessHandler() {
    return new LogoutHandler("/login.html");
  }

  public ExceptionMappingAuthenticationFailureHandler authenticationFailureHandler() {
    ExceptionMappingAuthenticationFailureHandler exceptionMappingAuthenticationFailureHandler =
        new ExceptionMappingAuthenticationFailureHandler();
    Map<String, String> exceptionMappings = new HashMap<>();
    exceptionMappings.put("org.springframework.security.authentication.BadCredentialsException",
        "/login.html?error=BAD_CREDS");
    exceptionMappings.put("org.springframework.security.authentication.CredentialsExpiredException",
        "/login.html?error=CRED_EXP");
    exceptionMappings.put("org.springframework.security.authentication.LockedException",
        "/login.html?error=ACC_LOCKED");
    exceptionMappings.put("org.springframework.security.authentication.DisabledException",
        "/login.html?error=ACC_DISABLED");
    exceptionMappingAuthenticationFailureHandler.setExceptionMappings(exceptionMappings);
    return exceptionMappingAuthenticationFailureHandler;
  }

  public void configureSecurity(HttpSecurity httpSecurity) throws Exception {
    httpSecurity.csrf().disable();

    httpSecurity.authorizeRequests()
        .mvcMatchers(
            "/login.html", "/authenticateUser", "/pulseVersion")
        .permitAll();

    httpSecurity.authorizeRequests()
        .mvcMatchers(
            "/accessDenied*")
        .authenticated();

    httpSecurity.authorizeRequests()
        .mvcMatchers(
            "/dataBrowser*", "/getQueryStatisticsGridModel/*")
        .access("hasRole('CLUSTER:READ') and hasRole('DATA:READ')");

    httpSecurity.authorizeRequests()
        .mvcMatchers(
            "/*")
        .hasRole("CLUSTER:READ");

    httpSecurity.formLogin()
        .loginPage("/login.html")
        .failureHandler(authenticationFailureHandler())
        .defaultSuccessUrl("/clusterDetail.html", true);

    httpSecurity.logout()
        .logoutUrl("/clusterLogout")
        .logoutSuccessHandler(customLogoutSuccessHandler());

    httpSecurity.headers()
        .frameOptions().deny();

    httpSecurity.headers()
        .xssProtection().xssProtectionEnabled(true).block(true);

    httpSecurity.headers()
        .contentTypeOptions();
  }

  @Bean
  @Profile("pulse.authentication.default")
  public WebSecurityConfigurerAdapter basic() {
    return new WebSecurityConfigurerAdapter() {
      @Bean
      @Override
      public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
      }

      @Override
      protected void configure(HttpSecurity httpSecurity) throws Exception {
        configureSecurity(httpSecurity);
      }

      @Override
      protected void configure(AuthenticationManagerBuilder authenticationManagerBuilder)
          throws Exception {
        authenticationManagerBuilder.inMemoryAuthentication()
            .withUser("admin")
            .password("admin")
            .roles("CLUSTER:READ,DATA:READ");
      }
    };
  }

  @Bean
  @Profile("pulse.authentication.gemfire")
  public WebSecurityConfigurerAdapter gemfire() {
    return new WebSecurityConfigurerAdapter() {
      @Bean
      @Override
      public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
      }

      @Override
      protected void configure(HttpSecurity httpSecurity) throws Exception {
        configureSecurity(httpSecurity);
      }

      @Override
      protected void configure(AuthenticationManagerBuilder authenticationManagerBuilder) {
        authenticationManagerBuilder.authenticationProvider(gemAuthenticationProvider());
      }
    };
  }
}
