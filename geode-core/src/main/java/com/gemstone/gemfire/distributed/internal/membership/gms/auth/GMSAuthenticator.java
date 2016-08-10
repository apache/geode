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
package com.gemstone.gemfire.distributed.internal.membership.gms.auth;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static com.gemstone.gemfire.internal.i18n.LocalizedStrings.*;

import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Authenticator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.security.IntegratedSecurityService;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.GemFireSecurityException;

public class GMSAuthenticator implements Authenticator {

  private Services services;
  private Properties securityProps;
  private SecurityService securityService = IntegratedSecurityService.getSecurityService();

  @Override
  public void init(Services s) {
    this.services = s;
    this.securityProps = this.services.getConfig().getDistributionConfig().getSecurityProps();
  }

  @Override
  public void start() {
  }

  @Override
  public void started() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void stopped() {
  }

  @Override
  public void installView(NetView v) {
  }

  @Override
  public void beSick() {
  }

  @Override
  public void playDead() {
  }

  @Override
  public void beHealthy() {
  }
  
  @Override
  public void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect, String reason) {
  }

  /**
   * Authenticate peer member with authenticator class defined by property
   * "security-peer-authenticator".
   *
   * @param  member
   *         the member to be authenticated
   * @param  credentials
   *         the credentials used in authentication
   * @return null if authentication succeed (including no authenticator case),
   *         otherwise, return failure message
   * @throws AuthenticationFailedException
   *         this will be removed since return string is used for failure
   */
  @Override
  public String authenticate(InternalDistributedMember member, Properties credentials) throws AuthenticationFailedException {
    return authenticate(member, credentials, this.securityProps, this.services.getJoinLeave().getMemberID());
  }

  /**
   * Method is package protected to be used in testing.
   */
  String authenticate(DistributedMember member, Properties credentials, Properties secProps, DistributedMember localMember) throws AuthenticationFailedException {
    if (!this.securityService.isPeerSecurityRequired()) {
      return null;
    }

    InternalLogWriter securityLogWriter = this.services.getSecurityLogWriter();

    if(credentials == null){
      securityLogWriter.warning(AUTH_PEER_AUTHENTICATION_MISSING_CREDENTIALS, member);
      return AUTH_PEER_AUTHENTICATION_MISSING_CREDENTIALS.toLocalizedString(member);
    }

    String failMsg = null;
    try {
      if(this.securityService.isIntegratedSecurity()){
        String username = credentials.getProperty("security-username");
        String password = credentials.getProperty("security-password");
        this.securityService.login(username, password);
      }
      else {
        invokeAuthenticator(secProps, member, credentials);
      }
    } catch (Exception ex) {
      securityLogWriter.warning(AUTH_PEER_AUTHENTICATION_FAILED_WITH_EXCEPTION, new Object[] {
        member, ex.getLocalizedMessage()
      }, ex);
      failMsg = AUTH_PEER_AUTHENTICATION_FAILED.toLocalizedString(localMember);
    }
    return failMsg;
  }


  /**
   * Method is package protected to be used in testing.
   */
  Principal invokeAuthenticator(Properties securityProps, DistributedMember member, Properties credentials) throws AuthenticationFailedException {
      String authMethod = securityProps.getProperty(SECURITY_PEER_AUTHENTICATOR);
    com.gemstone.gemfire.security.Authenticator auth = null;
    try {
      auth = SecurityService.getObjectOfTypeFromFactoryMethod(authMethod, com.gemstone.gemfire.security.Authenticator.class);

      LogWriter logWriter = this.services.getLogWriter();
      LogWriter securityLogWriter = this.services.getSecurityLogWriter();

      auth.init(this.securityProps, logWriter, securityLogWriter); // this.securityProps contains security-ldap-basedn but security-ldap-baseDomainName is expected
      return auth.authenticate(credentials, member);

    } catch (GemFireSecurityException gse) {
      throw gse;

    } catch (Exception ex) {
      throw new AuthenticationFailedException(HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT.toLocalizedString(), ex);

    } finally {
      if (auth != null) auth.close();
    }
  }

  /**
   * Get credential object for the given GemFire distributed member.
   *
   * @param  member
   *         the target distributed member
   * @return the credentials
   */
  @Override
  public Properties getCredentials(InternalDistributedMember member) {
    try {
      return getCredentials(member, securityProps);

    } catch (Exception e) {
      String authMethod = securityProps.getProperty(SECURITY_PEER_AUTH_INIT);
      services.getSecurityLogWriter().warning(LocalizedStrings.AUTH_FAILED_TO_OBTAIN_CREDENTIALS_IN_0_USING_AUTHINITIALIZE_1_2, new Object[] { authMethod, e.getLocalizedMessage() });
      return null;
    }
  }

  /**
   * For testing only.
   */
  Properties getCredentials(DistributedMember member, Properties secProps) {
    Properties credentials = null;
    String authMethod = secProps.getProperty(SECURITY_PEER_AUTH_INIT);

    try {
      if (authMethod != null && authMethod.length() > 0) {
        AuthInitialize auth = SecurityService.getObjectOfType(authMethod, AuthInitialize.class);
        assert auth != null;
        try {
          LogWriter logWriter = services.getLogWriter();
          LogWriter securityLogWriter = services.getSecurityLogWriter();
          auth.init(logWriter, securityLogWriter);
          credentials = auth.getCredentials(secProps, member, true);
        } finally {
          auth.close();
        }
      }

    } catch (GemFireSecurityException gse) {
      throw gse;

    } catch (Exception ex) {
      throw new AuthenticationRequiredException(HandShake_FAILED_TO_ACQUIRE_AUTHINITIALIZE_METHOD_0.toLocalizedString(authMethod), ex);
    }

    return credentials;
  }

  /**
   * For testing only.
   */
  Properties getSecurityProps() {
    return this.securityProps;
  }

  @Override
  public void emergencyClose() {
  }
}
