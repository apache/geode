/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package templates.security;

import java.security.Principal;
import java.security.Provider;
import java.security.Security;
import java.util.Properties;

import com.gemstone.gemfire.util.test.TestUtil;

/**
 * @author kneeraj
 * 
 */
public class PKCSCredentialGenerator extends CredentialGenerator {

  public static String keyStoreDir = getKeyStoreDir();

  public static boolean usesIBMJSSE;

  // Checks if the current JVM uses only IBM JSSE providers.
  private static boolean usesIBMProviders() {
    Provider[] providers = Security.getProviders();
    for (int index = 0; index < providers.length; ++index) {
      if (!providers[index].getName().toLowerCase().startsWith("ibm")) {
        return false;
      }
    }
    return true;
  }

  private static String getKeyStoreDir() {
    usesIBMJSSE = usesIBMProviders();
    if (usesIBMJSSE) {
      return "/lib/keys/ibm";
    }
    else {
      return "/lib/keys";
    }
  }

  public ClassCode classCode() {
    return ClassCode.PKCS;
  }

  public String getAuthInit() {
    return "templates.security.PKCSAuthInit.create";
  }

  public String getAuthenticator() {
    return "templates.security.PKCSAuthenticator.create";
  }

  public Properties getInvalidCredentials(int index) {
    Properties props = new Properties();
    String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, keyStoreDir + "/gemfire11.keystore");
    props.setProperty(PKCSAuthInit.KEYSTORE_FILE_PATH, keyStoreFile);
    props.setProperty(PKCSAuthInit.KEYSTORE_ALIAS, "gemfire11");
    props.setProperty(PKCSAuthInit.KEYSTORE_PASSWORD, "gemfire");
    return props;
  }

  public Properties getValidCredentials(int index) {
    Properties props = new Properties();
    int aliasnum = (index % 10) + 1;
    String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, keyStoreDir + "/gemfire" + aliasnum + ".keystore");
    props.setProperty(PKCSAuthInit.KEYSTORE_FILE_PATH, keyStoreFile);
    props.setProperty(PKCSAuthInit.KEYSTORE_ALIAS, "gemfire" + aliasnum);
    props.setProperty(PKCSAuthInit.KEYSTORE_PASSWORD, "gemfire");
    return props;
  }

  public Properties getValidCredentials(Principal principal) {
    Properties props = new Properties();
    String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, keyStoreDir + principal.getName() + ".keystore");
    props.setProperty(PKCSAuthInit.KEYSTORE_FILE_PATH, keyStoreFile);
    props.setProperty(PKCSAuthInit.KEYSTORE_ALIAS, principal.getName());
    props.setProperty(PKCSAuthInit.KEYSTORE_PASSWORD, "gemfire");
    return props;
  }

  protected Properties initialize() throws IllegalArgumentException {
    Properties props = new Properties();
    String keyStoreFile = TestUtil.getResourcePath(PKCSCredentialGenerator.class, keyStoreDir + "/publickeyfile");
    props.setProperty(PKCSAuthenticator.PUBLIC_KEY_FILE, keyStoreFile);
    props.setProperty(PKCSAuthenticator.PUBLIC_KEYSTORE_PASSWORD, "gemfire");
    return props;
  }

}
