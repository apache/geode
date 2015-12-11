
package templates.security;

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
