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



import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;

import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Spring security AuthenticationProvider for GemFire. It connects to gemfire manager using given
 * credentials. Successful connect is treated as successful authentication and web user is
 * authenticated
 *
 * @since GemFire version 9.0
 */
public class OauthAuthenticationProvider implements AuthenticationProvider {
  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {
    // at initial login, initialize the cluster of the this user
    Repository.get().getCluster();
    return authentication;
  }

  @Override
  public boolean supports(Class<?> authentication) {
    return authentication.equals(OAuth2AuthenticationToken.class);
  }

}
