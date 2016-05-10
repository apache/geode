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
package com.gemstone.gemfire.management.internal.cli.commands;

import static com.gemstone.gemfire.distributed.internal.DistributionConfig.*;
import static com.gemstone.gemfire.management.internal.cli.i18n.CliStrings.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.util.test.TestUtil.*;

import java.io.File;
import java.util.Properties;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @since  8.1
 */
@Category({ DistributedTest.class, SecurityTest.class })
public class ConnectCommandWithHttpAndSSLDUnitTest extends CliCommandTestBase {

  private static final ThreadLocal<Properties> sslInfoHolder = new ThreadLocal<>();

  private File jks;

  // TODO: should this test use @RunWith(Parameterized.class)?

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
    this.jks = new File(getResourcePath(getClass(), "/ssl/trusted.keystore"));
  }
  
  @Override
  protected final void preTearDownCliCommandTestBase() throws Exception {
    destroyDefaultSetup();
  }
  
  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    sslInfoHolder.set(null);
  }

  @Test
  public void testMutualAuthentication() throws Exception {
    Properties serverProps = new Properties();
    serverProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    serverProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    serverProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    serverProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME, "JKS");
    serverProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME, "SSL");
    serverProps.setProperty(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME, "true");
    serverProps.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE_NAME, jks.getCanonicalPath());
    serverProps.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME, "password");

    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__KEY_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__KEY_STORE_PASSWORD, "password");
    clientProps.setProperty(CONNECT__SSL_PROTOCOLS, "SSL");
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");

    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(serverProps);
  }

  @Test
  public void testSimpleSSL() throws Exception {
    Properties serverProps = new Properties();
    serverProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    serverProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    serverProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    serverProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME, "JKS");

    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(serverProps);
  }

  @Test
  public void testSSLWithoutKeyStoreType() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
  
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Test
  public void testSSLWithSSLProtocol() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME,"SSL");
    
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Test
  public void testSSLWithTLSProtocol() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLS");
    
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Test
  public void testSSLWithTLSv11Protocol() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.1");
    
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Test
  public void testSSLWithTLSv12Protocol() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.2");
    
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Test
  public void testWithMultipleProtocol() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME,"SSL,TLSv1.2");
    
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Ignore("disabled for unknown reason")
  @Test
  public void testSSLWithCipherSuite() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME, "TLSv1.2");

    //Its bad to hard code here. But using SocketFactory.getDefaultCiphers() somehow is not working with the option 
    //"https.cipherSuites" which is required to restrict cipher suite with HttpsURLConnection
    //Keeping the below code for further investigation on different Java versions ( 7 & 8) @TODO
    
   /*SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    
    sslContext.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = sslContext.getSocketFactory().getSupportedCipherSuites();*/
    
    localProps.setProperty(HTTP_SERVICE_SSL_CIPHERS_NAME,"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");

    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    clientProps.setProperty(CONNECT__SSL_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
    clientProps.setProperty(CONNECT__SSL_PROTOCOLS, "TLSv1.2");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Ignore("disabled for unknown reason")
  @Test
  public void testSSLWithMultipleCipherSuite() throws Exception {
    System.setProperty("javax.net.debug", "ssl,handshake,failure");
    
    Properties localProps = new Properties();
    localProps.setProperty(HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.2");
    localProps.setProperty(HTTP_SERVICE_SSL_CIPHERS_NAME,"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_EMPTY_RENEGOTIATION_INFO_SCSV");
    
    Properties clientProps = new Properties();
    clientProps.setProperty(CONNECT__TRUST_STORE, jks.getCanonicalPath());
    clientProps.setProperty(CONNECT__TRUST_STORE_PASSWORD, "password");
    clientProps.setProperty(CONNECT__SSL_PROTOCOLS, "TLSv1.2");
    
    sslInfoHolder.set(clientProps);
    setUpJmxManagerOnVm0ThenConnect(localProps);
  }

  @Override
  protected void connect(final String host, final int jmxPort, final int httpPort, final HeadlessGfsh shell) {
    assertNotNull(host);
    assertNotNull(shell);

    final CommandStringBuilder command = new CommandStringBuilder(CONNECT);
    String endpoint;

    // This is for testing purpose only. If we remove this piece of code we will
    // get a java.security.cert.CertificateException
    // as matching hostname can not be obtained in all test environment.
    HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
      @Override
      public boolean verify(String string, SSLSession ssls) {
        return true;
      }
    });
    
    endpoint = "https://" + host + ":" + httpPort + "/gemfire/v1";
    
    command.addOption(CONNECT__USE_HTTP, Boolean.TRUE.toString());
    command.addOption(CONNECT__URL, endpoint);
    command.addOption(CONNECT__USE_SSL,Boolean.TRUE.toString());

    if(sslInfoHolder.get().getProperty(CONNECT__KEY_STORE) != null){
      command.addOption(CONNECT__KEY_STORE, sslInfoHolder.get().getProperty(CONNECT__KEY_STORE));
    }
    if(sslInfoHolder.get().getProperty(CONNECT__KEY_STORE_PASSWORD) != null){
      command.addOption(CONNECT__KEY_STORE_PASSWORD, sslInfoHolder.get().getProperty(CONNECT__KEY_STORE_PASSWORD));
    }
    if(sslInfoHolder.get().getProperty(CONNECT__TRUST_STORE) != null){
      command.addOption(CONNECT__TRUST_STORE, sslInfoHolder.get().getProperty(CONNECT__TRUST_STORE));
    }
    if(sslInfoHolder.get().getProperty(CONNECT__TRUST_STORE_PASSWORD) != null){
      command.addOption(CONNECT__TRUST_STORE_PASSWORD, sslInfoHolder.get().getProperty(CONNECT__TRUST_STORE_PASSWORD));
    }
    if(sslInfoHolder.get().getProperty(CONNECT__SSL_PROTOCOLS) != null){
      command.addOption(CONNECT__SSL_PROTOCOLS, sslInfoHolder.get().getProperty(CONNECT__SSL_PROTOCOLS));
    }
    if(sslInfoHolder.get().getProperty(CONNECT__SSL_CIPHERS) != null){
      command.addOption(CONNECT__SSL_CIPHERS, sslInfoHolder.get().getProperty(CONNECT__SSL_CIPHERS));
    }

    CommandResult result = executeCommand(shell, command.toString());

    if (!shell.isConnectedAndReady()) {
      fail("Connect command failed to connect to manager " + endpoint + " result=" + commandResultToString(result));
    }

    info("Successfully connected to managing node using HTTPS");
    assertEquals(true, shell.isConnectedAndReady());
  }

}
