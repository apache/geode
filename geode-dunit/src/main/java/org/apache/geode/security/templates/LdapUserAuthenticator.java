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
package org.apache.geode.security.templates;

import java.security.Principal;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;

/**
 * An implementation of {@link Authenticator} that uses LDAP.
 *
 * @since GemFire 5.5
 */
public class LdapUserAuthenticator implements Authenticator {

  private static final Logger logger = LogService.getLogger();

  public static final String LDAP_SERVER_NAME = "security-ldap-server";
  public static final String LDAP_BASEDN_NAME = "security-ldap-basedn";
  public static final String LDAP_SSL_NAME = "security-ldap-usessl";

  private String ldapServer = null;
  private String baseDomainName = null;
  private String ldapUrlScheme = null;

  public static Authenticator create() {
    return new LdapUserAuthenticator();
  }

  @Override
  public void init(final Properties securityProps, final LogWriter systemLogWriter,
      final LogWriter securityLogWriter) throws AuthenticationFailedException {
    logger.info("Initializing LdapUserAuthenticator with {}", securityProps);

    this.ldapServer = securityProps.getProperty(LDAP_SERVER_NAME);
    if (this.ldapServer == null || this.ldapServer.length() == 0) {
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: LDAP server property [" + LDAP_SERVER_NAME + "] not specified");
    }

    this.baseDomainName = securityProps.getProperty(LDAP_BASEDN_NAME);
    if (this.baseDomainName == null || this.baseDomainName.length() == 0) {
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: LDAP base DN property [" + LDAP_BASEDN_NAME + "] not specified");
    }

    final String sslName = securityProps.getProperty(LDAP_SSL_NAME);
    if (sslName != null && sslName.toLowerCase().equals("true")) {
      this.ldapUrlScheme = "ldaps://";
    } else {
      this.ldapUrlScheme = "ldap://";
    }
  }

  @Override
  public Principal authenticate(final Properties credentials, final DistributedMember member) {
    final String userName = credentials.getProperty(UserPasswordAuthInit.USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException("LdapUserAuthenticator: user name property ["
          + UserPasswordAuthInit.USER_NAME + "] not provided");
    }

    String password = credentials.getProperty(UserPasswordAuthInit.PASSWORD);
    if (password == null) {
      password = "";
    }

    final Properties env = new Properties();
    env.put(Context.INITIAL_CONTEXT_FACTORY, com.sun.jndi.ldap.LdapCtxFactory.class.getName());
    env.put(Context.PROVIDER_URL, this.ldapUrlScheme + this.ldapServer + '/' + this.baseDomainName);
    env.put(Context.SECURITY_PRINCIPAL, "uid=" + userName + "," + this.baseDomainName);
    env.put(Context.SECURITY_CREDENTIALS, password);

    try {
      final DirContext ctx = new InitialDirContext(env);
      ctx.close();
    } catch (Exception e) {
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: Failure with provided username, password combination for user name: "
              + userName,
          e);
    }

    return new UsernamePrincipal(userName);
  }

  @Override
  public void close() {}
}
