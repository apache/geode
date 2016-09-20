/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.tools.pulse.internal.security;

import java.util.Collection;
import javax.management.remote.JMXConnector;

import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;

import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;

/**
 * Spring security AuthenticationProvider for GemFire. It connects to gemfire manager using given credentials.
 * Successful connect is treated as successful authentication and web user is authenticated
 * @since GemFire version 9.0
 */
public class GemFireAuthenticationProvider implements AuthenticationProvider {

  private final static PulseLogWriter LOGGER = PulseLogWriter.getLogger();

  public GemFireAuthenticationProvider() {
    System.out.println("here");
  }

  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {

    if (authentication instanceof GemFireAuthentication) {
      GemFireAuthentication gemAuth = (GemFireAuthentication) authentication;
      LOGGER.fine("GemAuthentication is connected? = " + gemAuth.getJmxc());
      if (gemAuth.getJmxc() != null && gemAuth.isAuthenticated()) return gemAuth;
    }

    String name = authentication.getName();
    String password = authentication.getCredentials().toString();

    try {
      LOGGER.fine("Connecting to GemFire with user=" + name);
      JMXConnector jmxc = Repository.get().getCluster(name, password).connectToGemFire();
      if (jmxc != null) {
        Collection<GrantedAuthority> list = GemFireAuthentication.populateAuthorities(jmxc);
        GemFireAuthentication auth = new GemFireAuthentication(authentication.getPrincipal(),
            authentication.getCredentials(), list, jmxc);
        LOGGER.fine("For user " + name + " authList=" + list);
        return auth;
      } else {
        throw new AuthenticationServiceException("JMX Connection unavailable");
      }
    } catch (Exception e) {
      throw new BadCredentialsException("Error connecting to GemFire JMX Server", e);
    }
  }

  @Override
  public boolean supports(Class<?> authentication) {
    return authentication.equals(UsernamePasswordAuthenticationToken.class);
  }

}
