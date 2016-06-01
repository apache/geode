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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Authenticator;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.GemFireSecurityException;

import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import static com.gemstone.gemfire.internal.i18n.LocalizedStrings.*;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

// static messages

public class GMSAuthenticator implements Authenticator {

  private final static String secPrefix = DistributionConfig.GEMFIRE_PREFIX + "sys.security-";
  private final static int gemfireSysPrefixLen = (DistributionConfig.GEMFIRE_PREFIX + "sys.").length();

  private Services services;
  private Properties securityProps = getSecurityProps();

  @Override
  public void init(Services s) {
    this.services = s;
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
  public String authenticate(InternalDistributedMember member, Object credentials) throws AuthenticationFailedException {
    return authenticate(member, credentials, this.securityProps, this.services.getJoinLeave().getMemberID());
  }

  /**
   * Method is package protected to be used in testing.
   */
  String authenticate(DistributedMember member, Object credentials, Properties secProps, DistributedMember localMember) throws AuthenticationFailedException {

    String authMethod = secProps.getProperty(SECURITY_PEER_AUTHENTICATOR);
    if (authMethod == null || authMethod.length() == 0) {
      return null;
    }

    InternalLogWriter securityLogWriter = this.services.getSecurityLogWriter();
    String failMsg = null;
    if (credentials != null) {
      try {
        invokeAuthenticator(authMethod, member, credentials);

      } catch (Exception ex) {
        securityLogWriter.warning(AUTH_PEER_AUTHENTICATION_FAILED_WITH_EXCEPTION, new Object[] {member, authMethod, ex.getLocalizedMessage()}, ex);
        failMsg = AUTH_PEER_AUTHENTICATION_FAILED.toLocalizedString(localMember);
      }

    } else { // No credentials - need to send failure message
      securityLogWriter.warning(AUTH_PEER_AUTHENTICATION_MISSING_CREDENTIALS, new Object[] {member, authMethod});
      failMsg = AUTH_PEER_AUTHENTICATION_MISSING_CREDENTIALS.toLocalizedString(member, authMethod);
    }

    return failMsg;
  }

  /**
   * Method is package protected to be used in testing.
   */
  Principal invokeAuthenticator(String authMethod, DistributedMember member, Object credentials) throws AuthenticationFailedException {
    com.gemstone.gemfire.security.Authenticator auth = null;

    try {
      Method getter = ClassLoadUtil.methodFromName(authMethod);
      auth = (com.gemstone.gemfire.security.Authenticator) getter.invoke(null, (Object[]) null);
      if (auth == null) {
        throw new AuthenticationFailedException(HandShake_AUTHENTICATOR_INSTANCE_COULD_NOT_BE_OBTAINED.toLocalizedString());
      }

      LogWriter logWriter = this.services.getLogWriter();
      LogWriter securityLogWriter = this.services.getSecurityLogWriter();

      auth.init(this.securityProps, logWriter, securityLogWriter); // this.securityProps contains security-ldap-basedn but security-ldap-baseDomainName is expected
      return auth.authenticate((Properties) credentials, member);

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
   * @return the credential object
   */
  @Override
  public Object getCredentials(InternalDistributedMember member) {
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
        Method getter = ClassLoadUtil.methodFromName(authMethod);
        AuthInitialize auth = (AuthInitialize)getter.invoke(null, (Object[]) null);
        if (auth == null) {
          throw new AuthenticationRequiredException(AUTH_FAILED_TO_ACQUIRE_AUTHINITIALIZE_INSTANCE.toLocalizedString(authMethod));
        }

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

  Properties getSecurityProps() {
    Properties props = new Properties();
    Set keys = System.getProperties().keySet();
    for (Object key: keys) {
      String propKey = (String) key;
      if (propKey.startsWith(secPrefix)) {
        props.setProperty(propKey.substring(gemfireSysPrefixLen), System.getProperty(propKey));
      }
    }
    return props;
  }

  @Override
  public void emergencyClose() {
  }
}
