
package com.gemstone.gemfire.security;

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


import java.io.File;
import java.util.Properties;

import javax.net.ssl.SSLHandshakeException;

import security.CredentialGenerator;
import security.DummyCredentialGenerator;
import security.LdapUserCredentialGenerator;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests peer to peer authentication in Gemfire
 * 
 * @author Yogesh Mahajan
 * @since 5.5
 */
public class P2PAuthenticationDUnitTest extends DistributedTestCase {

  private static VM locatorVM = null;

  public static final String USER_NAME = "security-username";

  public static final String PASSWORD = "security-password";

  private static final String[] expectedExceptions = {
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      GemFireSecurityException.class.getName(),
      SSLHandshakeException.class.getName(),
      ClassNotFoundException.class.getName(),
      "Authentication failed for",
      "Failed to obtain credentials"};

  public P2PAuthenticationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {

    super.setUp();
    final Host host = Host.getHost(0);
    locatorVM = host.getVM(0);
  }

  private void setProperty(Properties props, String key, String value) {

    if (key != null && value != null) {
      props.setProperty(key, value);
    }
  }

  /**
   * Check that mcast-port setting for discovery or with locator are
   * incompatible with security
   */
  public void testIllegalPropertyCombos() throws Exception {

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File logFile = new File(getUniqueName() + "-locator" + port + ".log");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "26753");
    props.setProperty(DistributionConfig.LOCATORS_NAME, 
                      DistributedTestCase.getIPLiteral() + "[" + port + "]");
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        "templates.security.UserPasswordAuthInit.create");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    try {
      Locator.startLocatorAndDS(port, logFile, null, props);
      fail("Expected an IllegalArgumentException while starting locator");
    }
    catch (IllegalArgumentException ex) {
      // success
    }

    // Also try setting the authenticator
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "26753");
    props.setProperty(DistributionConfig.LOCATORS_NAME, 
                      DistributedTestCase.getIPLiteral() +"[" + port + "]");
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        "templates.security.LdapUserAuthenticator.create");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    try {
      Locator.startLocatorAndDS(port, logFile, null, props);
      fail("Expected an IllegalArgumentException while starting locator");
    }
    catch (IllegalArgumentException ex) {
      // success
    }

    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "26753");
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        "templates.security.UserPasswordAuthInit.create");
    try {
      getSystem(props);
      fail("Expected an IllegalArgumentException while connection to DS");
    }
    catch (IllegalArgumentException ex) {
      // success
    }

    // Also try setting the authenticator
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "26753");
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        "templates.security.LdapUserAuthenticator.create");
    try {
      getSystem(props);
      fail("Expected an IllegalArgumentException while connection to DS");
    }
    catch (IllegalArgumentException ex) {
      // success
    }
  }

  // AuthInitialize is incorrect
  public void testP2PAuthenticationWithInvalidAuthInitialize() throws Exception {

    disconnectAllFromDS();
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties props = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    if (props == null) {
      props = new Properties();
    }
    String authInit = " Incorrect_AuthInitialize";
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() + "[" + port + "]";
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
            authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
            authenticator);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[]{
            getUniqueName(), new Integer(port), props, javaProps,
            expectedExceptions});

    LogWriter dsLogger = createLogWriter(props);
    SecurityTestUtil.addExpectedExceptions(expectedExceptions, dsLogger);
    try {
      new SecurityTestUtil("tmp").createSystem(props, null);
      fail("AuthenticationFailedException was expected as the AuthInitialize object passed is incorrect");
    } catch (AuthenticationFailedException expected) {
      // success
    } finally {
      SecurityTestUtil.removeExpectedExceptions(expectedExceptions, dsLogger);
      locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[]{
              new Integer(port), expectedExceptions});
    }

  }

  // Authenticator is incorrect
  public void testP2PAuthenticationWithInvalidAuthenticator() throws Exception {
    disconnectAllFromDS();
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties props = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = "xyz";
    String authInit = gen.getAuthInit();
    if (props == null) {
      props = new Properties();
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() +"["+port+"]";
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
            authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
            authenticator);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[] {
            getUniqueName(), new Integer(port), props, javaProps,
            expectedExceptions });

    LogWriter dsLogger = createLogWriter(props);
    SecurityTestUtil.addExpectedExceptions(expectedExceptions, dsLogger);
    try {
      new SecurityTestUtil("tmp").createSystem(props, javaProps);
      fail("AuthenticationFailedException was expected as the Authenticator object passed is incorrect");
    }
    catch (AuthenticationFailedException expected) {
      // success
    }
    finally {
      SecurityTestUtil.removeExpectedExceptions(expectedExceptions, dsLogger);
      locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[] {
              new Integer(port), expectedExceptions });
    }
  }

  public void testP2PAuthenticationWithNoCredentials() throws Exception {

    disconnectAllFromDS();

    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties props = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();
    if (props == null) {
      props = new Properties();
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() +"["+port+"]";
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
            authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
            authenticator);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[] {
            getUniqueName(), new Integer(port), props, javaProps,
            expectedExceptions });

    LogWriter dsLogger = createLogWriter(props);
    SecurityTestUtil.addExpectedExceptions(expectedExceptions, dsLogger);
    try {
      new SecurityTestUtil("tmp").createSystem(props, null);
      fail("AuthenticationFailedException was expected as no credentials are set");
    }
    catch (AuthenticationFailedException expected) {
      // success
    }
    finally {
      SecurityTestUtil.removeExpectedExceptions(expectedExceptions, dsLogger);
      locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[] {
              new Integer(port), expectedExceptions });
    }
  }

  public void testP2PAuthenticationWithValidCredentials() throws Exception {

    disconnectAllFromDS();
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties props = gen.getSystemProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();
    if (props == null) {
      props = new Properties();
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() +"["+port+"]";
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
            authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
            authenticator);
    Properties credentials = gen.getValidCredentials(1);
    Properties javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[] {
            getUniqueName(), new Integer(port), props, javaProps,
            expectedExceptions });
    try {
      createDS(props, javaProps);
      verifyMembers(new Integer(2));
      disconnectFromDS();

    } finally {
      locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[] {
              new Integer(port), expectedExceptions });
    }
  }

  public void testP2PAuthenticationWithBothValidAndInValidCredentials()
      throws Exception {

    disconnectAllFromDS();
    addExpectedException("Authentication failed");

    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties props = gen.getSystemProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();
    if (props == null) {
      props = new Properties();
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() +"["+port+"]";
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
            authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
            authenticator);
    // valid credentials for locator
    Properties credentials = gen.getValidCredentials(1);
    Properties javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[] {
            getUniqueName(), new Integer(port), props, javaProps,
            expectedExceptions });
    try {
      // invalid credentials for the peer
      credentials = gen.getInvalidCredentials(1);
      javaProps = gen.getJavaProperties();
      props.putAll(credentials);

      LogWriter dsLogger = createLogWriter(props);
      SecurityTestUtil.addExpectedExceptions(expectedExceptions, dsLogger);
      try {
        new SecurityTestUtil("tmp").createSystem(props, javaProps);
        fail("AuthenticationFailedException was expected as wrong credentials were passed");
      }
      catch (AuthenticationFailedException expected) {
        // success
      }
      finally {
        SecurityTestUtil.removeExpectedExceptions(expectedExceptions, dsLogger);
      }

      credentials = gen.getValidCredentials(3);
      javaProps = gen.getJavaProperties();
      props.putAll(credentials);
      createDS(props, javaProps);
      verifyMembers(new Integer(2));
      disconnectFromDS();

    } finally {
      locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[] {
              new Integer(port), expectedExceptions });
    }
  }

  /**
   * The strategy is to test view change reject by having two different
   * authenticators on different VMs.
   * 
   * Here locator will accept the credentials from peer2 but the first peer will
   * reject them due to different authenticator. Hence the number of members
   * reported by the first peer should be only two while others will report as
   * three.
   */
  public void disabled_testP2PViewChangeReject() throws Exception {

    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    final VM peer2 = host.getVM(1);
    final VM peer3 = host.getVM(2);

    CredentialGenerator gen = new LdapUserCredentialGenerator();
    gen.init();
    Properties extraProps = gen.getSystemProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();
    if (extraProps == null) {
      extraProps = new Properties();
    }

    CredentialGenerator gen2 = new DummyCredentialGenerator();
    gen2.init();
    Properties extraProps2 = gen2.getSystemProperties();
    String authenticator2 = gen2.getAuthenticator();
    if (extraProps2 == null) {
      extraProps2 = new Properties();
    }

    // Start the locator with the LDAP authenticator
    Properties props = new Properties();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() +"["+port+"]";
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        authenticator);
    Properties credentials = gen.getValidCredentials(1);
    Properties javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    props.putAll(extraProps);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[] {
        getUniqueName(), new Integer(port), props, javaProps,
        expectedExceptions });
    try {

    // Start the first peer with different authenticator
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        authenticator2);
    credentials = gen.getValidCredentials(3);
    Properties javaProps2 = gen2.getJavaProperties();
    props.putAll(credentials);
    props.putAll(extraProps2);
    createDS(props, javaProps2);

    // Start the second peer with the same authenticator as locator
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        authenticator);
    credentials = gen.getValidCredentials(7);
    javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    props.putAll(extraProps);
    peer2.invoke(P2PAuthenticationDUnitTest.class, "createDS", new Object[] {
        props, javaProps });

    // Start the third peer with the same authenticator as locator
    peer3.invoke(P2PAuthenticationDUnitTest.class, "createDS", new Object[] {
        props, javaProps });

    // wait for view propagation
    pause(2000);
    // Verify the number of members on all peers and locator
    locatorVM.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(4) });
    verifyMembers(new Integer(2));
    peer2.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(4) });
    peer3.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(4) });

    // Disconnect the first peer and check again
    disconnectFromDS();
    pause(2000);
    locatorVM.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(3) });
    peer2.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(3) });
    peer3.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(3) });

    // Disconnect the second peer and check again
    peer2.invoke(DistributedTestCase.class, "disconnectFromDS");
    pause(2000);
    locatorVM.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(2) });
    peer3.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(2) });

    // Same for last peer
    peer3.invoke(DistributedTestCase.class, "disconnectFromDS");
    pause(2000);
    locatorVM.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(1) });

    } finally {
    locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[] {
        new Integer(port), expectedExceptions });
    }
  }

  /**
   * The strategy is to test credential size greater than UDP datagram size.
   * 
   * @see Bug # 38570.
   * 
   * Here locator will accept the credentials from peer2 and the large credential
   * from the first peer. Number of members in the DS
   * should be four
   */
  public void testP2PLargeCredentialSucceeds() throws Exception {

    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    final VM peer2 = host.getVM(1);
    final VM peer3 = host.getVM(2);

    CredentialGenerator gen = new DummyCredentialGenerator();
    gen.init();
    Properties extraProps = gen.getSystemProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = "security.UserPasswordWithExtraPropsAuthInit.create";
    if (extraProps == null) {
      extraProps = new Properties();
    }

    // Start the locator with the Dummy authenticator
    Properties props = new Properties();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = DistributedTestCase.getIPLiteral() +"["+port+"]";
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        authenticator);
    Properties credentials = gen.getValidCredentials(1);
    Properties javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    props.putAll(extraProps);
    locatorVM.invoke(SecurityTestUtil.class, "startLocator", new Object[] {
        getUniqueName(), new Integer(port), props, javaProps,
        expectedExceptions });
    try {

    // Start the first peer with huge credentials
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        authenticator);
    credentials = gen.getValidCredentials(3);
    javaProps = gen.getJavaProperties();
    String hugeStr = "20KString";
    for (int i = 0; i <= 20000; i++) {
      hugeStr += "A";
    }
    credentials.setProperty("security-keep-extra-props", "-");
    credentials.setProperty("security-hugeentryone", hugeStr);
    credentials.setProperty("security-hugeentrytwo", hugeStr);
    credentials.setProperty("security-hugeentrythree", hugeStr);

    props.putAll(credentials);
    props.putAll(extraProps);

    LogWriter dsLogger = createLogWriter(props);
    SecurityTestUtil.addExpectedExceptions(
        new String[] { IllegalArgumentException.class.getName() }, dsLogger);
    try {
      createDS(props, javaProps);
//      fail("AuthenticationFailedException was expected as credentials were passed beyond 50k");
    }
    finally {
      SecurityTestUtil.removeExpectedExceptions(
          new String[] { IllegalArgumentException.class.getName() }, dsLogger);
    }

    // Start the second peer with the same authenticator as locator
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
        authInit);
    setProperty(props, DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
        authenticator);
    credentials = gen.getValidCredentials(7);
    javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    props.putAll(extraProps);
    peer2.invoke(P2PAuthenticationDUnitTest.class, "createDS", new Object[] {
        props, javaProps });

    // Start the third peer with the same authenticator as locator
    peer3.invoke(P2PAuthenticationDUnitTest.class, "createDS", new Object[] {
        props, javaProps });

    // wait for view propagation
    pause(2000);
    // Verify the number of members on all peers and locator
    locatorVM.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(4) });
    peer2.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(4) });
    peer3.invoke(P2PAuthenticationDUnitTest.class, "verifyMembers",
        new Object[] { new Integer(4) });


    // Disconnect the peers
    disconnectFromDS();
    peer2.invoke(DistributedTestCase.class, "disconnectFromDS");
    peer3.invoke(DistributedTestCase.class, "disconnectFromDS");

    } finally {
    // Stopping the locator
    locatorVM.invoke(SecurityTestUtil.class, "stopLocator", new Object[] {
        new Integer(port), expectedExceptions });
    }
  }

  public static void createDS(Properties props, Object javaProps) {

    SecurityTestUtil tmpUtil = new SecurityTestUtil("tmp");
    tmpUtil.createSystem(props, (Properties)javaProps);
  }

  public static void verifyMembers(Integer numExpectedMembers) {

    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    MembershipManager mgr = MembershipManagerHelper
        .getMembershipManager(ds);
    assertEquals(numExpectedMembers.intValue(), mgr.getView().size());
  }

}
