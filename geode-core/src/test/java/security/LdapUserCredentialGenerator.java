/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package security;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.tier.sockets.HandShake;
import com.gemstone.gemfire.util.test.TestUtil;
import templates.security.LdapUserAuthenticator;
import templates.security.UserPasswordAuthInit;

import java.security.Principal;
import java.util.Properties;
import java.util.Random;

public class LdapUserCredentialGenerator extends CredentialGenerator {

  private static final String USER_PREFIX = "gemfire";

  private static boolean enableServerAuthentication = false;

  private boolean serverAuthEnabled = false;

  private static final Random prng = new Random();

  private static final String[] algos = new String[] { "", "DESede", "AES:128",
      "Blowfish:128" };

  public LdapUserCredentialGenerator() {
    // Toggle server authentication enabled for each test
    // This is done instead of running all the tests with both
    // server auth enabled/disabled to reduce test run time.
    enableServerAuthentication = !enableServerAuthentication;
    serverAuthEnabled = enableServerAuthentication;
  }

  @Override
  protected Properties initialize() throws IllegalArgumentException {

    Properties extraProps = new Properties();
    String ldapServer = System.getProperty("gf.ldap.server", "ldap");
    String ldapBaseDN = System.getProperty("gf.ldap.basedn", "ou=ldapTesting,dc=pune,dc=gemstone,dc=com");
    String ldapUseSSL = System.getProperty("gf.ldap.usessl");
    extraProps.setProperty(LdapUserAuthenticator.LDAP_SERVER_NAME, ldapServer);
    extraProps.setProperty(LdapUserAuthenticator.LDAP_BASEDN_NAME, ldapBaseDN);
    if (ldapUseSSL != null && ldapUseSSL.length() > 0) {
      extraProps.setProperty(LdapUserAuthenticator.LDAP_SSL_NAME, ldapUseSSL);
    }
    if (serverAuthEnabled) {
      String keyStoreFile = TestUtil.getResourcePath(LdapUserCredentialGenerator.class, PKCSCredentialGenerator.keyStoreDir + "/gemfire1.keystore");
      extraProps.setProperty(HandShake.PRIVATE_KEY_FILE_PROP, keyStoreFile);
      extraProps.setProperty(HandShake.PRIVATE_KEY_ALIAS_PROP, "gemfire1");
      extraProps.setProperty(HandShake.PRIVATE_KEY_PASSWD_PROP, "gemfire");
    }
    return extraProps;
  }

  @Override
  public ClassCode classCode() {
    return ClassCode.LDAP;
  }

  @Override
  public String getAuthInit() {
    return templates.security.UserPasswordAuthInit.class.getName() + ".create";
  }

  @Override
  public String getAuthenticator() {
    return templates.security.LdapUserAuthenticator.class.getName() + ".create";
  }

  @Override
  public Properties getValidCredentials(int index) {

    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, USER_PREFIX
        + ((index % 10) + 1));
    props.setProperty(UserPasswordAuthInit.PASSWORD, USER_PREFIX
        + ((index % 10) + 1));
    props.setProperty(DistributionConfig.SECURITY_CLIENT_DHALGO_NAME,
        algos[prng.nextInt(algos.length)]);
    if (serverAuthEnabled) {
      String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, PKCSCredentialGenerator.keyStoreDir + "/publickeyfile");
      props.setProperty(HandShake.PUBLIC_KEY_FILE_PROP, keyStoreFile);
      props.setProperty(HandShake.PUBLIC_KEY_PASSWD_PROP, "gemfire");
    }
    return props;
  }

  @Override
  public Properties getValidCredentials(Principal principal) {

    Properties props = null;
    String userName = principal.getName();
    if (userName != null && userName.startsWith(USER_PREFIX)) {
      boolean isValid;
      try {
        int suffix = Integer.parseInt(userName.substring(USER_PREFIX.length()));
        isValid = (suffix >= 1 && suffix <= 10);
      }
      catch (Exception ex) {
        isValid = false;
      }
      if (isValid) {
        props = new Properties();
        props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
        props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
      }
    }
    if (props == null) {
      throw new IllegalArgumentException("LDAP: [" + userName
          + "] not a valid user");
    }
    props.setProperty(DistributionConfig.SECURITY_CLIENT_DHALGO_NAME,
        algos[prng.nextInt(algos.length)]);
    if (serverAuthEnabled) {
      String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, PKCSCredentialGenerator.keyStoreDir + "/publickeyfile");
      props.setProperty(HandShake.PUBLIC_KEY_FILE_PROP, keyStoreFile);
      props.setProperty(HandShake.PUBLIC_KEY_PASSWD_PROP, "gemfire");
    }
    return props;
  }

  @Override
  public Properties getInvalidCredentials(int index) {

    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, "invalid" + index);
    props.setProperty(UserPasswordAuthInit.PASSWORD, "none");
    props.setProperty(DistributionConfig.SECURITY_CLIENT_DHALGO_NAME,
        algos[prng.nextInt(algos.length)]);
    if (serverAuthEnabled) {
      String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, PKCSCredentialGenerator.keyStoreDir + "/publickeyfile");
      props.setProperty(HandShake.PUBLIC_KEY_FILE_PROP, keyStoreFile);
      props.setProperty(HandShake.PUBLIC_KEY_PASSWD_PROP, "gemfire");
    }
    return props;
  }

}
