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
package com.gemstone.gemfire.security;

import java.io.IOException;
import java.util.Properties;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import security.CredentialGenerator;
import security.CredentialGenerator.ClassCode;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

import security.DummyCredentialGenerator;

/**
 * Test for authentication from client to server. This tests for both valid and
 * invalid credentials/modules. It also checks for authentication
 * success/failure in case of failover and for the notification channel.
 * 
 * @author sumedh
 * @since 5.5
 */
public class ClientAuthenticationDUnitTest extends DistributedTestCase {

  /** constructor */
  public ClientAuthenticationDUnitTest(String name) {
    super(name);
  }

  private VM server1 = null;

  private VM server2 = null;

  private VM client1 = null;

  private VM client2 = null;

  private static final String[] serverExpectedExceptions = {
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      GemFireSecurityException.class.getName(),
      ClassNotFoundException.class.getName(), IOException.class.getName(),
      SSLException.class.getName(), SSLHandshakeException.class.getName() };

  private static final String[] clientExpectedExceptions = {
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      SSLHandshakeException.class.getName() };

  @Override
  public void setUp() throws Exception {

    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
    
    IgnoredException.addIgnoredException("Connection refused: connect");

    server1.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { serverExpectedExceptions });
    server2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { serverExpectedExceptions });
    client1.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { clientExpectedExceptions });
    client2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { clientExpectedExceptions });
  }

  // Region: Utility and static functions invoked by the tests

  public static Integer createCacheServer(Object dsPort, Object locatorString,
      Object authenticator, Object extraProps, Object javaProps) {

    Properties authProps;
    if (extraProps == null) {
      authProps = new Properties();
    }
    else {
      authProps = (Properties)extraProps;
    }
    if (authenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, authenticator
              .toString());
    }
    return SecurityTestUtil.createCacheServer(authProps, javaProps,
        (Integer)dsPort, (String)locatorString, null, new Integer(
            SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createCacheServer(Object dsPort, Object locatorString,
      Integer serverPort, Object authenticator, Object extraProps,
      Object javaProps) {

    Properties authProps;
    if (extraProps == null) {
      authProps = new Properties();
    }
    else {
      authProps = (Properties)extraProps;
    }
    if (authenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, authenticator
              .toString());
    }
    SecurityTestUtil.createCacheServer(authProps, javaProps, (Integer)dsPort,
        (String)locatorString, serverPort, new Integer(
            SecurityTestUtil.NO_EXCEPTION));
  }

  private static void createCacheClient(Object authInit, Properties authProps,
      Properties javaProps, Integer[] ports, Object numConnections,
      Boolean multiUserMode, Boolean subscriptionEnabled, Integer expectedResult) {

    String authInitStr = (authInit == null ? null : authInit.toString());
    SecurityTestUtil.createCacheClient(authInitStr, authProps, javaProps,
        ports, (Integer)numConnections, Boolean.FALSE,
        multiUserMode.toString(), subscriptionEnabled, expectedResult);
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer[] ports, Object numConnections,
      Boolean multiUserMode, Integer expectedResult) {

    createCacheClient(authInit, (Properties)authProps, (Properties)javaProps,
        ports, numConnections, multiUserMode, Boolean.TRUE, expectedResult);
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer port1, Object numConnections,
      Integer expectedResult) {

    createCacheClient(authInit, (Properties)authProps, (Properties)javaProps,
        new Integer[] { port1 }, numConnections, Boolean.FALSE, Boolean.TRUE,
        expectedResult);
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer port1, Integer port2, Object numConnections,
      Integer expectedResult) {
    createCacheClient(authInit, authProps, javaProps, port1, port2,
        numConnections, Boolean.FALSE, expectedResult);
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer port1, Integer port2, Object numConnections,
      Boolean multiUserMode, Integer expectedResult) {

    createCacheClient(authInit, authProps, javaProps,
        port1, port2, numConnections, multiUserMode, Boolean.TRUE,
        expectedResult);
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer port1, Integer port2, Object numConnections,
      Boolean multiUserMode, Boolean subscriptionEnabled,
      Integer expectedResult) {

    createCacheClient(authInit, (Properties)authProps, (Properties)javaProps,
        new Integer[] { port1, port2 }, numConnections, multiUserMode,
        subscriptionEnabled, expectedResult);
  }

  public static void registerAllInterest() {

    Region region = SecurityTestUtil.getCache().getRegion(
        SecurityTestUtil.regionName);
    assertNotNull(region);
    region.registerInterestRegex(".*");
  }

  // End Region: Utility and static functions invoked by the tests

  // Region: Tests

  public void testValidCredentials() {
    itestValidCredentials(Boolean.FALSE);
  }

  public void itestValidCredentials(Boolean multiUser) {
      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter().info(
          "testValidCredentials: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testValidCredentials: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter().info("testValidCredentials: Using authinit: " + authInit);

      // Start the servers
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = (Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps });
      Integer port2 = (Integer)server2.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, authenticator, extraProps,
              javaProps });

      // Start the clients with valid credentials
      Properties credentials1 = gen.getValidCredentials(1);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testValidCredentials: For first client credentials: " + credentials1
              + " : " + javaProps1);
      Properties credentials2 = gen.getValidCredentials(2);
      Properties javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testValidCredentials: For second client credentials: "
              + credentials2 + " : " + javaProps2);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials2, javaProps2, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts",
          new Object[] { new Integer(2) });

      // Verify that the puts succeeded
      client2.invoke(SecurityTestUtil.class, "doGets",
          new Object[] { new Integer(2) });
      
      if (multiUser) {
        client1.invoke(SecurityTestUtil.class, "doProxyCacheClose");
        client2.invoke(SecurityTestUtil.class, "doProxyCacheClose");
        client1.invoke(SecurityTestUtil.class, "doSimplePut",
            new Object[] {"CacheClosedException"});
        client2.invoke(SecurityTestUtil.class, "doSimpleGet",
            new Object[] {"CacheClosedException"});
      }
  }

  public void testNoCredentials() {
    itestNoCredentials(Boolean.FALSE);
  }

  public void itestNoCredentials(Boolean multiUser) {
      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter()
          .info("testNoCredentials: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testNoCredentials: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter().info("testNoCredentials: Using authinit: " + authInit);

      // Start the servers
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps }));
      Integer port2 = ((Integer)server2.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, authenticator, extraProps,
              javaProps }));

      // Start first client with valid credentials
      Properties credentials1 = gen.getValidCredentials(1);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testNoCredentials: For first client credentials: " + credentials1
              + " : " + javaProps1);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts",
          new Object[] { new Integer(2) });

      // Trying to create the region on client2 
      if (gen.classCode().equals(ClassCode.SSL)) {
        // For SSL the exception may not come since the server can close socket
        // before handshake message is sent from client. However exception
        // should come in any region operations.
        client2
            .invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
                new Object[] { null, null, null, port1, port2, null, multiUser,
                    new Integer(SecurityTestUtil.NO_EXCEPTION) });
        client2.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
            new Integer(2), new Integer(SecurityTestUtil.OTHER_EXCEPTION) });
      }
      else {
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, null, null, port1, port2,
                null, multiUser, new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
      }
  }

  public void testInvalidCredentials() {
    itestInvalidCredentials(Boolean.FALSE);
  }

  public void itestInvalidCredentials(Boolean multiUser) {


      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter().info(
          "testInvalidCredentials: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testInvalidCredentials: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter()
          .info("testInvalidCredentials: Using authinit: " + authInit);

      // Start the servers
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps }));
      Integer port2 = ((Integer)server2.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, authenticator, extraProps,
              javaProps }));

      // Start first client with valid credentials
      Properties credentials1 = gen.getValidCredentials(1);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidCredentials: For first client credentials: "
              + credentials1 + " : " + javaProps1);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts",
          new Object[] { new Integer(2) });

      // Start second client with invalid credentials
      // Trying to create the region on client2 should throw a security
      // exception
      Properties credentials2 = gen.getInvalidCredentials(1);
      Properties javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidCredentials: For second client credentials: "
              + credentials2 + " : " + javaProps2);
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials2, javaProps2, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });
  }

  public void testInvalidAuthInit() {
    itestInvalidAuthInit(Boolean.FALSE);
  }

  public void itestInvalidAuthInit(Boolean multiUser) {

      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();

      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthInit: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthInit: Using authenticator: " + authenticator);

      // Start the server
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps }));

      Properties credentials = gen.getValidCredentials(1);
      javaProps = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthInit: For first client credentials: " + credentials
              + " : " + javaProps);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { "com.gemstone.none", credentials, javaProps,
            new Integer[] { port1 }, null, multiUser,
            Integer.valueOf(SecurityTestUtil.AUTHREQ_EXCEPTION) });
  }

  public void testNoAuthInitWithCredentials() {
    itestNoAuthInitWithCredentials(Boolean.FALSE);
  }

  public void itestNoAuthInitWithCredentials(Boolean multiUser) {

      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();


      LogWriterUtils.getLogWriter().info(
          "testNoAuthInitWithCredentials: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testNoAuthInitWithCredentials: Using authenticator: "
              + authenticator);

      // Start the servers
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps }));
      Integer port2 = ((Integer)server2.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, authenticator, extraProps,
              javaProps }));

      // Start the clients with valid credentials
      Properties credentials1 = gen.getValidCredentials(1);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testNoAuthInitWithCredentials: For first client credentials: "
              + credentials1 + " : " + javaProps1);
      Properties credentials2 = gen.getValidCredentials(2);
      Properties javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testNoAuthInitWithCredentials: For second client credentials: "
              + credentials2 + " : " + javaProps2);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { null, credentials1, javaProps1, port1, port2, null,
          multiUser, new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { null, credentials2, javaProps2, port1, port2, null,
          multiUser, new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
      client2.invoke(SecurityTestUtil.class, "closeCache");
      

      // Now also try with invalid credentials
      credentials2 = gen.getInvalidCredentials(5);
      javaProps2 = gen.getJavaProperties();
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { null, credentials2, javaProps2, port1, port2, null,
          multiUser, new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
  }

  public void testInvalidAuthenticator() {
    itestInvalidAuthenticator(Boolean.FALSE);
  }

  public void itestInvalidAuthenticator(Boolean multiUser) {

      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthenticator: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthenticator: Using authinit: " + authInit);

      // Start the server with invalid authenticator
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = (Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, "com.gemstone.gemfire.none",
              extraProps, javaProps });

      // Trying to create the region on client should throw a security exception
      Properties credentials = gen.getValidCredentials(1);
      javaProps = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthenticator: For first client credentials: "
              + credentials + " : " + javaProps);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials, javaProps, port1, null,
              new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });
      client1.invoke(SecurityTestUtil.class, "closeCache");
      

      // Also test with invalid credentials
      credentials = gen.getInvalidCredentials(1);
      javaProps = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidAuthenticator: For first client credentials: "
              + credentials + " : " + javaProps);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials, javaProps, port1, null,
              new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });
  }

  public void testNoAuthenticatorWithCredentials() {
    itestNoAuthenticatorWithCredentials(Boolean.FALSE);
  }

  public void itestNoAuthenticatorWithCredentials(Boolean multiUser) {

      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter().info(
          "testNoAuthenticatorWithCredentials: Using scheme: "
              + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testNoAuthenticatorWithCredentials: Using authinit: " + authInit);

      // Start the servers with no authenticator
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = (Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, null, extraProps, javaProps });
      Integer port2 = (Integer)server2.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, null, extraProps, javaProps });

      // Clients should connect successfully and work properly with
      // valid/invalid credentials when none are required on the server side
      Properties credentials1 = gen.getValidCredentials(3);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testNoAuthenticatorWithCredentials: For first client credentials: "
              + credentials1 + " : " + javaProps1);
      Properties credentials2 = gen.getInvalidCredentials(5);
      Properties javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testNoAuthenticatorWithCredentials: For second client credentials: "
              + credentials2 + " : " + javaProps2);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials2, javaProps2, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts",
          new Object[] { new Integer(2) });

      // Verify that the puts succeeded
      client2.invoke(SecurityTestUtil.class, "doGets",
          new Object[] { new Integer(2) });
  }

  public void testCredentialsWithFailover() {
    itestCredentialsWithFailover(Boolean.FALSE);
  }

  public void itestCredentialsWithFailover(Boolean multiUser) {
      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter().info(
          "testCredentialsWithFailover: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testCredentialsWithFailover: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter().info(
          "testCredentialsWithFailover: Using authinit: " + authInit);

      // Start the first server
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = (Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps });
      // Get a port for second server but do not start it
      // This forces the clients to connect to the first server
      Integer port2 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));

      // Start the clients with valid credentials
      Properties credentials1 = gen.getValidCredentials(5);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testCredentialsWithFailover: For first client credentials: "
              + credentials1 + " : " + javaProps1);
      Properties credentials2 = gen.getValidCredentials(6);
      Properties javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testCredentialsWithFailover: For second client credentials: "
              + credentials2 + " : " + javaProps2);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials2, javaProps2, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts",
          new Object[] { new Integer(2) });
      // Verify that the puts succeeded
      client2.invoke(SecurityTestUtil.class, "doGets",
          new Object[] { new Integer(2) });

      // start the second one and stop the first server to force a failover
      server2.invoke(ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, port2, authenticator, extraProps,
              javaProps });
      server1.invoke(SecurityTestUtil.class, "closeCache");

      // Perform some create/update operations from client1
      client1.invoke(SecurityTestUtil.class, "doNPuts",
          new Object[] { new Integer(4) });
      // Verify that the creates/updates succeeded
      client2.invoke(SecurityTestUtil.class, "doNGets",
          new Object[] { new Integer(4) });

      // Try to connect client2 with no credentials
      // Verify that the creation of region throws security exception
      if (gen.classCode().equals(ClassCode.SSL)) {
        // For SSL the exception may not come since the server can close socket
        // before handshake message is sent from client. However exception
        // should come in any region operations.
        client2
            .invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
                new Object[] { null, null, null, port1, port2, null, multiUser,
                    new Integer(SecurityTestUtil.NOFORCE_AUTHREQ_EXCEPTION) });
        client2.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
            new Integer(2), new Integer(SecurityTestUtil.OTHER_EXCEPTION) });
      }
      else {
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, null, null, port1, port2,
                null, multiUser, new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
      }

      // Now try to connect client1 with invalid credentials
      // Verify that the creation of region throws security exception
      credentials1 = gen.getInvalidCredentials(7);
      javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testCredentialsWithFailover: For first client invalid credentials: "
              + credentials1 + " : " + javaProps1);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });

      if (multiUser) {
        client1.invoke(SecurityTestUtil.class, "doProxyCacheClose");
        client2.invoke(SecurityTestUtil.class, "doProxyCacheClose");
        client1.invoke(SecurityTestUtil.class, "doSimplePut",
            new Object[] {"CacheClosedException"});
        client2.invoke(SecurityTestUtil.class, "doSimpleGet",
            new Object[] {"CacheClosedException"});
      }
  }

  public void testCredentialsForNotifications() {
    itestCredentialsForNotifications(Boolean.FALSE);
  }

  public void itestCredentialsForNotifications(Boolean multiUser) {
      CredentialGenerator gen = new DummyCredentialGenerator();
      Properties extraProps = gen.getSystemProperties();
      Properties javaProps = gen.getJavaProperties();
      String authenticator = gen.getAuthenticator();
      String authInit = gen.getAuthInit();

      LogWriterUtils.getLogWriter().info(
          "testCredentialsForNotifications: Using scheme: " + gen.classCode());
      LogWriterUtils.getLogWriter().info(
          "testCredentialsForNotifications: Using authenticator: "
              + authenticator);
      LogWriterUtils.getLogWriter().info(
          "testCredentialsForNotifications: Using authinit: " + authInit);

      // Start the first server
      Integer locPort1 = SecurityTestUtil.getLocatorPort();
      Integer locPort2 = SecurityTestUtil.getLocatorPort();
      String locString = SecurityTestUtil.getLocatorString();
      Integer port1 = (Integer)server1.invoke(
          ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, authenticator, extraProps,
              javaProps });
      // Get a port for second server but do not start it
      // This forces the clients to connect to the first server
      Integer port2 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));

      // Start the clients with valid credentials
      Properties credentials1 = gen.getValidCredentials(3);
      Properties javaProps1 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testCredentialsForNotifications: For first client credentials: "
              + credentials1 + " : " + javaProps1);
      Properties credentials2 = gen.getValidCredentials(4);
      Properties javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testCredentialsForNotifications: For second client credentials: "
              + credentials2 + " : " + javaProps2);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials1, javaProps1, port1, port2,
              null, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });
      // Set up zero forward connections to check notification handshake only
      Object zeroConns = new Integer(0);
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials2, javaProps2, port1, port2,
              zeroConns, multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Register interest on all keys on second client
      client2
          .invoke(ClientAuthenticationDUnitTest.class, "registerAllInterest");

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts",
          new Object[] { new Integer(2) });

      // Verify that the puts succeeded
      client2.invoke(SecurityTestUtil.class, "doLocalGets",
          new Object[] { new Integer(2) });

      // start the second one and stop the first server to force a failover
      server2.invoke(ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort2, locString, port2, authenticator, extraProps,
              javaProps });
      server1.invoke(SecurityTestUtil.class, "closeCache");

      // Wait for failover to complete
      Wait.pause(500);

      // Perform some create/update operations from client1
      client1.invoke(SecurityTestUtil.class, "doNPuts",
          new Object[] { new Integer(4) });
      // Verify that the creates/updates succeeded
      client2.invoke(SecurityTestUtil.class, "doNLocalGets",
          new Object[] { new Integer(4) });

      // Try to connect client1 with no credentials
      // Verify that the creation of region throws security exception
      server1.invoke(ClientAuthenticationDUnitTest.class, "createCacheServer",
          new Object[] { locPort1, locString, port1, authenticator, extraProps,
              javaProps });
      if (gen.classCode().equals(ClassCode.SSL)) {
        // For SSL the exception may not come since the server can close socket
        // before handshake message is sent from client. However exception
        // should come in any region operations.
        client1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, null, null, port1, port2,
                zeroConns, multiUser,
                new Integer(SecurityTestUtil.NOFORCE_AUTHREQ_EXCEPTION) });
        client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
            new Integer(2), new Integer(SecurityTestUtil.OTHER_EXCEPTION) });
      }
      else {
        client1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, null, null, port1, port2,
                zeroConns, multiUser, new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
      }

      // Now try to connect client2 with invalid credentials
      // Verify that the creation of region throws security exception
      credentials2 = gen.getInvalidCredentials(3);
      javaProps2 = gen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testCredentialsForNotifications: For second client invalid credentials: "
              + credentials2 + " : " + javaProps2);
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, credentials2, javaProps2, port1, port2,
              zeroConns, multiUser, new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });

      // Now try to connect client2 with invalid auth-init method
      // Trying to create the region on client with valid credentials should
      // throw a security exception
      client2
          .invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
              new Object[] { "com.gemstone.none", credentials1, javaProps1,
                  port1, port2, zeroConns, multiUser, 
                  new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });

      // Now start the servers with invalid authenticator method.
      // Skip this test for a scheme which does not have an authInit in the
      // first place (e.g. SSL) since that will fail with AuthReqEx before
      // authenticator is even invoked.
      if (authInit != null && authInit.length() > 0) {
        server1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheServer", new Object[] { locPort1, locString, port1,
                "com.gemstone.gemfire.none", extraProps, javaProps });
        server2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheServer", new Object[] { locPort2, locString, port2,
                "com.gemstone.gemfire.none", extraProps, javaProps });

        // Trying to create the region on client with valid/invalid credentials
        // should throw a security exception
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { authInit, credentials1,
                javaProps1, port1, port2, zeroConns, multiUser,
                new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });
        client1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { authInit, credentials2,
                javaProps2, port1, port2, zeroConns, multiUser,
                new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });
      }
      else {
        LogWriterUtils.getLogWriter().info(
            "testCredentialsForNotifications: Skipping invalid authenticator for scheme ["
                + gen.classCode() + "] which has no authInit");
      }

      // Try connection with null auth-init on clients.
      // Skip this test for a scheme which does not have an authInit in the
      // first place (e.g. SSL).
      if (authInit != null && authInit.length() > 0) {
        server1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheServer", new Object[] { locPort1, locString, port1,
                authenticator, extraProps, javaProps });
        server2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheServer", new Object[] { locPort2, locString, port2,
                authenticator, extraProps, javaProps });
        client1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, credentials1, javaProps1,
                port1, port2, null, multiUser,
                new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, credentials2, javaProps2,
                port1, port2, zeroConns, multiUser,
                new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });

        // Now also try with invalid credentials on client2
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { null, credentials2, javaProps2,
                port1, port2, zeroConns, multiUser,
                new Integer(SecurityTestUtil.AUTHREQ_EXCEPTION) });
      }
      else {
        LogWriterUtils.getLogWriter().info(
            "testCredentialsForNotifications: Skipping null authInit for scheme ["
                + gen.classCode() + "] which has no authInit");
      }

      // Try connection with null authenticator on server and sending
      // valid/invalid credentials.
      // If the scheme does not have an authenticator in the first place (e.g.
      // SSL) then skip it since this test is useless.
      if (authenticator != null && authenticator.length() > 0) {
        server1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheServer", new Object[] { locPort1, locString, port1,
                null, extraProps, javaProps });
        server2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheServer", new Object[] { locPort2, locString, port2,
                null, extraProps, javaProps });
        client1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { authInit, credentials1,
                javaProps1, port1, port2, null, multiUser,
                new Integer(SecurityTestUtil.NO_EXCEPTION) });
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { authInit, credentials2,
                javaProps2, port1, port2, zeroConns, multiUser,
                new Integer(SecurityTestUtil.NO_EXCEPTION) });

        // Register interest on all keys on second client
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "registerAllInterest");

        // Perform some put operations from client1
        client1.invoke(SecurityTestUtil.class, "doPuts",
            new Object[] { new Integer(4) });

        // Verify that the puts succeeded
        client2.invoke(SecurityTestUtil.class, "doLocalGets",
            new Object[] { new Integer(4) });

        // Now also try with valid credentials on client2
        client1.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { authInit, credentials2,
                javaProps2, port1, port2, null, multiUser,
                new Integer(SecurityTestUtil.NO_EXCEPTION) });
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "createCacheClient", new Object[] { authInit, credentials1,
                javaProps1, port1, port2, zeroConns, multiUser,
                new Integer(SecurityTestUtil.NO_EXCEPTION) });

        // Register interest on all keys on second client
        client2.invoke(ClientAuthenticationDUnitTest.class,
            "registerAllInterest");

        // Perform some put operations from client1
        client1.invoke(SecurityTestUtil.class, "doNPuts",
            new Object[] { new Integer(4) });

        // Verify that the puts succeeded
        client2.invoke(SecurityTestUtil.class, "doNLocalGets",
            new Object[] { new Integer(4) });
      }
      else {
        LogWriterUtils.getLogWriter().info(
            "testCredentialsForNotifications: Skipping scheme ["
                + gen.classCode() + "] which has no authenticator");
      }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Tests for MULTI_USER_MODE start here
  //////////////////////////////////////////////////////////////////////////////

  public void xtestValidCredentialsForMultipleUsers() {
    itestValidCredentials(Boolean.TRUE);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Tests for MULTI_USER_MODE end here
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  protected final void preTearDown() throws Exception {
    // close the clients first
    client1.invoke(SecurityTestUtil.class, "closeCache");
    client2.invoke(SecurityTestUtil.class, "closeCache");
    // then close the servers
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");
  }
}
