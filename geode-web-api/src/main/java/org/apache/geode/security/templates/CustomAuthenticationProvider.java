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

package org.apache.geode.security.templates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.templates.SampleSecurityManager.Role;
import org.apache.geode.security.templates.SampleSecurityManager.User;
import org.apache.shiro.subject.Subject;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Component;

import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.internal.security.ResourceConstants;
import com.gemstone.gemfire.security.AuthenticationFailedException;

@Component
public class CustomAuthenticationProvider implements AuthenticationProvider {

  private static Collection<GrantedAuthority> populateAuthorities(User user) {
    Collection<GrantedAuthority> authorities = new ArrayList<>();
    for (Role role : user.getRoles()) {
      for (ResourcePermission permission : role.getPermissions())
        authorities.add(new SampleAuthority(permission.toString()));
    }
    return authorities;
  }

  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {
    String username = authentication.getName();
    String password = authentication.getCredentials().toString();

    Properties credentials = new Properties();
    credentials.put(ResourceConstants.USER_NAME, username);
    credentials.put(ResourceConstants.PASSWORD, password);
    SampleSecurityManager securityManager = (SampleSecurityManager) GeodeSecurityUtil.getSecurityManager();

    try {
      String authenticatedName = (String) securityManager.authenticate(credentials);
      SampleSecurityManager.User user = securityManager.getUser(authenticatedName);
      Subject subject = GeodeSecurityUtil.login(username, password);
      if (subject != null) {
        return new SampleAuthentication(subject.getPrincipal(), authentication.getCredentials(), populateAuthorities(user));
      }
    } catch (AuthenticationFailedException authFailedEx) {
      throw new BadCredentialsException("Invalid username or password");
    }
    return authentication;
  }

  @Override
  public boolean supports(Class<?> authentication) {
    return authentication.equals(UsernamePasswordAuthenticationToken.class);
  }
}