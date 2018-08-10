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
package org.apache.geode.security.generator;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_DHALGO;

import java.security.Principal;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.templates.LdapUserAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.util.test.TestUtil;

public class LdapUserCredentialGenerator extends CredentialGenerator {

  private static final Logger logger = LogService.getLogger();

  private static final String USER_PREFIX = "gemfire";
  private static final Random RANDOM = new Random();
  private static final String[] CIPHERS = new String[] {"", "DESede", "AES:128", "Blowfish:128"};

  private static boolean enableServerAuthentication = false;

  private boolean serverAuthEnabled = false;

  public LdapUserCredentialGenerator() {
    // Toggle server authentication enabled for each test
    // This is done instead of running all the tests with both
    // server auth enabled/disabled to reduce test run time.
    enableServerAuthentication = !enableServerAuthentication;
    this.serverAuthEnabled = enableServerAuthentication;
  }

  @Override
  protected Properties initialize() throws IllegalArgumentException {
    final String ldapServer = System.getProperty("gf.ldap.server", "ldap");
    final String ldapBaseDN =
        System.getProperty("gf.ldap.basedn", "ou=ldapTesting,dc=pune,dc=gemstone,dc=com");
    final String ldapUseSSL = System.getProperty("gf.ldap.usessl");

    final Properties extraProps = new Properties();
    extraProps.setProperty(LdapUserAuthenticator.LDAP_SERVER_NAME, ldapServer);
    extraProps.setProperty(LdapUserAuthenticator.LDAP_BASEDN_NAME, ldapBaseDN);

    if (ldapUseSSL != null && ldapUseSSL.length() > 0) {
      extraProps.setProperty(LdapUserAuthenticator.LDAP_SSL_NAME, ldapUseSSL);
    }

    if (serverAuthEnabled) {
      String keyStoreFile = TestUtil.getResourcePath(LdapUserCredentialGenerator.class,
          PKCSCredentialGenerator.keyStoreDir + "/gemfire1.keystore");
      extraProps.setProperty(Handshake.PRIVATE_KEY_FILE_PROP, keyStoreFile);
      extraProps.setProperty(Handshake.PRIVATE_KEY_ALIAS_PROP, DistributionConfig.GEMFIRE_PREFIX);
      extraProps.setProperty(Handshake.PRIVATE_KEY_PASSWD_PROP, "gemfire");
    }

    Assert.assertNotNull(extraProps.getProperty(LdapUserAuthenticator.LDAP_BASEDN_NAME));

    logger.info("Generating LdapUserCredentialGenerator with {}", extraProps);

    return extraProps;
  }

  @Override
  public ClassCode classCode() {
    return ClassCode.LDAP;
  }

  @Override
  public String getAuthInit() {
    return UserPasswordAuthInit.class.getName() + ".create";
  }

  @Override
  public String getAuthenticator() {
    return LdapUserAuthenticator.class.getName() + ".create";
  }

  @Override
  public Properties getValidCredentials(final int index) {
    final Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, USER_PREFIX + ((index % 10) + 1));
    props.setProperty(UserPasswordAuthInit.PASSWORD, USER_PREFIX + ((index % 10) + 1));
    props.setProperty(SECURITY_CLIENT_DHALGO, CIPHERS[RANDOM.nextInt(CIPHERS.length)]);

    if (serverAuthEnabled) {
      final String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class,
          PKCSCredentialGenerator.keyStoreDir + "/publickeyfile");
      props.setProperty(Handshake.PUBLIC_KEY_FILE_PROP, keyStoreFile);
      props.setProperty(Handshake.PUBLIC_KEY_PASSWD_PROP, "gemfire");
    }

    return props;
  }

  @Override
  public Properties getValidCredentials(final Principal principal) {
    Properties props = null;
    final String userName = principal.getName();

    if (userName != null && userName.startsWith(USER_PREFIX)) {
      boolean isValid;

      try {
        final int suffix = Integer.parseInt(userName.substring(USER_PREFIX.length()));
        isValid = (suffix >= 1 && suffix <= 10);
      } catch (Exception ex) {
        isValid = false;
      }

      if (isValid) {
        props = new Properties();
        props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
        props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
      }
    }

    if (props == null) {
      throw new IllegalArgumentException("LDAP: [" + userName + "] not a valid user");
    }

    props.setProperty(SECURITY_CLIENT_DHALGO, CIPHERS[RANDOM.nextInt(CIPHERS.length)]);

    if (serverAuthEnabled) {
      final String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class,
          PKCSCredentialGenerator.keyStoreDir + "/publickeyfile");
      props.setProperty(Handshake.PUBLIC_KEY_FILE_PROP, keyStoreFile);
      props.setProperty(Handshake.PUBLIC_KEY_PASSWD_PROP, "gemfire");
    }

    return props;
  }

  @Override
  public Properties getInvalidCredentials(final int index) {
    final Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, "invalid" + index);
    props.setProperty(UserPasswordAuthInit.PASSWORD, "none");
    props.setProperty(SECURITY_CLIENT_DHALGO, CIPHERS[RANDOM.nextInt(CIPHERS.length)]);

    if (serverAuthEnabled) {
      final String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class,
          PKCSCredentialGenerator.keyStoreDir + "/publickeyfile");
      props.setProperty(Handshake.PUBLIC_KEY_FILE_PROP, keyStoreFile);
      props.setProperty(Handshake.PUBLIC_KEY_PASSWD_PROP, "gemfire");
    }

    return props;
  }
}
