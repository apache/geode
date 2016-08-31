/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.rest.internal.web.security.template;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.geode.security.templates.CustomAuthenticationProvider;
import org.apache.geode.security.templates.SampleAccessDeniedHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
//@EnableGlobalMethodSecurity(securedEnabled = true)
@ComponentScan("org.apache.geode.security.templates")
public class SampleRESTSecurityConfiguration extends WebSecurityConfigurerAdapter {

  @Autowired
  private CustomAuthenticationProvider authProvider;

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
    http.authorizeRequests()
        .antMatchers("/ping", "/login", "/logout")
        .permitAll()
        .anyRequest()
        .authenticated()
        .and()
        .formLogin();
    http.exceptionHandling().authenticationEntryPoint(new AlwaysSendUnauthorized401AuthenticationEntryPoint());
    http.exceptionHandling().accessDeniedHandler(new SampleAccessDeniedHandler());
    http.csrf().disable();
    // TODO - figure out how to override this for other types of authentication
    http.httpBasic();
  }

  public class AlwaysSendUnauthorized401AuthenticationEntryPoint implements AuthenticationEntryPoint {

    @Override
    public final void commence(HttpServletRequest request,
                               HttpServletResponse response,
                               AuthenticationException authException) throws IOException {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    }
  }
}
