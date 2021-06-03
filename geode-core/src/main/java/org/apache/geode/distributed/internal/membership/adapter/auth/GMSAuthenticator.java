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
package org.apache.geode.distributed.internal.membership.adapter.auth;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;

import java.security.Principal;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.Authenticator;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.security.CallbackInstantiator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class GMSAuthenticator implements Authenticator<InternalDistributedMember> {

  private final Properties securityProps;
  private final SecurityService securityService;
  private final LogWriter securityLogWriter;
  private final LogWriter logWriter;

  public GMSAuthenticator(final Properties securityProps,
      final SecurityService securityService,
      final LogWriter securityLogWriter,
      final LogWriter logWriter) {
    this.securityProps = securityProps;
    this.securityService = securityService;
    this.securityLogWriter = securityLogWriter;
    this.logWriter = logWriter;
  }

  /**
   * Authenticate peer member with authenticator class defined by property
   * "security-peer-authenticator".
   *
   * @param member the member to be authenticated
   * @param credentials the credentials used in authentication
   * @return null if authentication succeed (including no authenticator case), otherwise, return
   *         failure message
   */
  @Override
  public String authenticate(InternalDistributedMember member, Properties credentials) {
    return authenticate(member, credentials, this.securityProps);
  }

  /**
   * Method is package protected to be used in testing.
   */
  String authenticate(MemberIdentifier member, Properties credentials, Properties secProps) {

    // For older systems, locator might be started without cache, so secureService may not be
    // initialized here. We need to check if the passed in secProps has peer authenticator or not at
    // this point
    String authMethod = secProps.getProperty(SECURITY_PEER_AUTHENTICATOR);
    if (!securityService.isPeerSecurityRequired() && StringUtils.isBlank(authMethod)) {
      return null;
    }

    if (credentials == null) {
      securityLogWriter.warning(String.format("Failed to find credentials from [%s]", member));
      return String.format("Failed to find credentials from [%s]", member);
    }

    String failMsg = null;
    try {
      if (securityService.isIntegratedSecurity()) {
        securityService.login(credentials);
        securityService.authorize(Resource.CLUSTER, Operation.MANAGE);
      } else {
        invokeAuthenticator(secProps, member, credentials);
      }
    } catch (Exception ex) {
      securityLogWriter.warning(String.format("Security check failed for [%s]. %s",
          member, ex.getLocalizedMessage()), ex);
      failMsg = String.format("Security check failed. %s", ex.getLocalizedMessage());
    }

    return failMsg;
  }


  /**
   * Method is package protected to be used in testing.
   */
  Principal invokeAuthenticator(Properties securityProps, MemberIdentifier member,
      Properties credentials) throws AuthenticationFailedException {
    String authMethod = securityProps.getProperty(SECURITY_PEER_AUTHENTICATOR);
    org.apache.geode.security.Authenticator auth = null;
    try {
      auth = CallbackInstantiator.getObjectOfType(authMethod,
          org.apache.geode.security.Authenticator.class);

      // this.securityProps contains security-ldap-basedn but security-ldap-baseDomainName is
      // expected
      auth.init(this.securityProps, logWriter, securityLogWriter);
      return auth.authenticate(credentials, (InternalDistributedMember) member);

    } catch (GemFireSecurityException gse) {
      throw gse;

    } catch (Exception ex) {
      throw new AuthenticationFailedException(
          "Failed to acquire Authenticator object", ex);

    } finally {
      if (auth != null) {
        auth.close();
      }
    }
  }

  /**
   * Get credential object for the given GemFire distributed member.
   *
   * @param member the target distributed member
   * @return the credentials
   */
  @Override
  public Properties getCredentials(InternalDistributedMember member) {
    try {
      return getCredentials(member, securityProps);

    } catch (Exception e) {
      String authMethod = securityProps.getProperty(SECURITY_PEER_AUTH_INIT);
      securityLogWriter.warning(
          String.format("Failed to obtain credentials using AuthInitialize [%s]. %s",
              new Object[] {authMethod, e.getLocalizedMessage()}));
      return null;
    }
  }

  /**
   * For testing only.
   */
  Properties getCredentials(MemberIdentifier member, Properties secProps) {
    String authMethod = secProps.getProperty(SECURITY_PEER_AUTH_INIT);
    return Handshake.getCredentials(authMethod, secProps,
        (InternalDistributedMember) member,
        true,
        logWriter,
        securityLogWriter);
  }

  /**
   * For testing only.
   */
  Properties getSecurityProps() {
    return this.securityProps;
  }

}
