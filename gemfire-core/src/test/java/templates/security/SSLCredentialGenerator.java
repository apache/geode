/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package templates.security;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.security.AuthenticationFailedException;

public class SSLCredentialGenerator extends CredentialGenerator {

  private File findTrustedJKS() {
    File ssldir = new File(System.getProperty("JTESTS") + "/ssl");
    return new File(ssldir, "trusted.keystore");
  }

  private File findUntrustedJKS() {
    File ssldir = new File(System.getProperty("JTESTS") + "/ssl");
    return new File(ssldir, "untrusted.keystore");
  }

  private Properties getValidJavaSSLProperties() {
    File jks = findTrustedJKS();
    try {
      Properties props = new Properties();
      props.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.trustStorePassword", "password");
      props.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.keyStorePassword", "password");
      return props;
    }
    catch (IOException ex) {
      throw new AuthenticationFailedException(
          "SSL: Exception while opening the key store: " + ex);
    }
  }

  private Properties getInvalidJavaSSLProperties() {
    File jks = findUntrustedJKS();
    try {
      Properties props = new Properties();
      props.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.trustStorePassword", "password");
      props.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.keyStorePassword", "password");
      return props;
    }
    catch (IOException ex) {
      throw new AuthenticationFailedException(
          "SSL: Exception while opening the key store: " + ex);
    }
  }

  private Properties getSSLProperties() {
    Properties props = new Properties();
    props.setProperty("ssl-enabled", "true");
    props.setProperty("ssl-require-authentication", "true");
    props.setProperty("ssl-ciphers", "SSL_RSA_WITH_RC4_128_MD5");
    props.setProperty("ssl-protocols", "TLSv1");
    return props;
  }

  protected Properties initialize() throws IllegalArgumentException {
    this.javaProps = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  public ClassCode classCode() {
    return ClassCode.SSL;
  }

  public String getAuthInit() {
    return null;
  }

  public String getAuthenticator() {
    return null;
  }

  public Properties getValidCredentials(int index) {
    this.javaProps = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  public Properties getValidCredentials(Principal principal) {
    this.javaProps = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  public Properties getInvalidCredentials(int index) {
    this.javaProps = getInvalidJavaSSLProperties();
    return getSSLProperties();
  }

}
