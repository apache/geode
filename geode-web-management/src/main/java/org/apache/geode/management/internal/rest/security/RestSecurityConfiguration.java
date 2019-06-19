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
package org.apache.geode.management.internal.rest.security;


import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import org.apache.geode.management.api.ClusterManagementResult;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
// this package name needs to be different than the admin rest controller's package name
// otherwise this component scan will pick up the admin rest controllers as well.
@ComponentScan("org.apache.geode.management.internal.rest")
public class RestSecurityConfiguration extends WebSecurityConfigurerAdapter {

  @Autowired
  private GeodeAuthenticationProvider authProvider;

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  protected void configure(AuthenticationManagerBuilder auth) throws Exception {
    auth.authenticationProvider(authProvider);
  }

  @Bean
  @Override
  public AuthenticationManager authenticationManagerBean() throws Exception {
    return super.authenticationManagerBean();
  }

  protected void configure(HttpSecurity http) throws Exception {
    http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and()
        .authorizeRequests()
        .antMatchers("/ping", "/docs/**", "/swagger-ui.html", "/v2/api-docs/**",
            "/webjars/springfox-swagger-ui/**", "/swagger-resources/**")
        .permitAll()
        .anyRequest().authenticated().and().csrf().disable();

    if (this.authProvider.getSecurityService().isIntegratedSecurity()) {
      http.httpBasic().authenticationEntryPoint(new AuthenticationEntryPoint() {
        @Override
        public void commence(HttpServletRequest request, HttpServletResponse response,
            AuthenticationException authException)
            throws IOException, ServletException {
          response.addHeader("WWW-Authenticate", "Basic realm=\"GEODE\"");
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          response.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
          ClusterManagementResult<?> result =
              new ClusterManagementResult<>(ClusterManagementResult.StatusCode.UNAUTHENTICATED,
                  authException.getMessage());
          objectMapper.writeValue(response.getWriter(), result);
        }
      });
    } else {
      http.authorizeRequests().anyRequest().permitAll();
    }
  }
}
