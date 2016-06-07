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
package com.gemstone.gemfire.internal.security.shiro;

import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.internal.security.ResourceConstants;
import com.gemstone.gemfire.security.ExternalSecurity;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.security.GeodePermission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

public class CustomAuthRealm extends AuthorizingRealm{
  public static final String REALM_NAME = "CUSTOMAUTHREALM";

  private static final Logger logger = LogManager.getLogger(CustomAuthRealm.class);
  private ExternalSecurity externalSecurity = null;

  public CustomAuthRealm(ExternalSecurity auth) {
    externalSecurity = auth;
  }


  public CustomAuthRealm (String authenticatorFactory) {
    Object auth = GeodeSecurityUtil.getAuthenticatorObject(authenticatorFactory);

    if(!(auth instanceof ExternalSecurity)){
      throw new GemFireSecurityException("Integrated Security requires ExternalSecurity interface.");
    }
    externalSecurity = (ExternalSecurity) auth;
  }

    @Override
  protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
    UsernamePasswordToken authToken = (UsernamePasswordToken) token;
    String username = authToken.getUsername();
    String password = new String(authToken.getPassword());

    Properties credentialProps = new Properties();
    credentialProps.put(ResourceConstants.USER_NAME, username);
    credentialProps.put(ResourceConstants.PASSWORD, password);

    Principal principal  = externalSecurity.authenticate(credentialProps);

    return new SimpleAuthenticationInfo(principal, authToken.getPassword(), REALM_NAME);
  }


  @Override
  protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
    // we intercepted the call to this method by overriding the isPermitted call
    return null;
  }

  @Override
  public boolean isPermitted(PrincipalCollection principals, Permission permission) {
    GeodePermission context = (GeodePermission) permission;
    Principal principal = (Principal) principals.getPrimaryPrincipal();
    return externalSecurity.authorize(principal, context);
  }

}
