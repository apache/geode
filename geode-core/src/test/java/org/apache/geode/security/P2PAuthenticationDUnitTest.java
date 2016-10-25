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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.AvailablePort.*;
import static org.apache.geode.security.SecurityTestUtils.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.IgnoredException.*;
import static org.apache.geode.test.dunit.NetworkUtils.*;
import static org.apache.geode.test.dunit.Wait.*;

import java.util.Properties;

import javax.net.ssl.SSLHandshakeException;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.security.generator.LdapUserCredentialGenerator;
import org.apache.geode.security.generator.UserPasswordWithExtraPropsAuthInit;
import org.apache.geode.security.templates.LdapUserAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Tests peer to peer authentication in Gemfire
 * 
 * @since GemFire 5.5
 */
@Category({ DistributedTest.class, SecurityTest.class })
public class P2PAuthenticationDUnitTest extends JUnit4DistributedTestCase {

  private static VM locatorVM = null;

  private static final String[] ignoredExceptions = {
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      GemFireSecurityException.class.getName(),
      SSLHandshakeException.class.getName(),
      ClassNotFoundException.class.getName(),
      "Authentication failed for",
      "Failed to obtain credentials"
  };

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    locatorVM = Host.getHost(0).getVM(0);
    for (String exceptionString : ignoredExceptions) {
      addIgnoredException(exceptionString);
    }
  }

  /**
   * Check that mcast-port setting for discovery or with locator are
   * incompatible with security
   */
  @Test
  public void testIllegalPropertyCombos() throws Exception {
    int port = getRandomAvailablePort(SOCKET);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "26753");
    props.setProperty(ConfigurationProperties.LOCATORS, getIPLiteral() + "[" + port + "]");
    props.setProperty(ConfigurationProperties.SECURITY_PEER_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    props.setProperty(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");

    try {
      Locator.startLocatorAndDS(port, null, null, props);
      fail("Expected an IllegalArgumentException while starting locator");

    } catch (IllegalArgumentException ex) {
      // success
    }

    // Also try setting the authenticator
    props = new Properties();
    props.setProperty(MCAST_PORT, "26753");
    props.setProperty(LOCATORS, getIPLiteral() + "[" + port + "]");
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, LdapUserAuthenticator.class.getName() + ".create");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    try {
      Locator.startLocatorAndDS(port, null, null, props);
      fail("Expected an IllegalArgumentException while starting locator");

    } catch (IllegalArgumentException expected) {
      // success
    }

    props = new Properties();
    props.setProperty(MCAST_PORT, "26753");
    props.setProperty(SECURITY_PEER_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");

    try {
      getSystem(props);
      fail("Expected an IllegalArgumentException while connection to DS");

    } catch (IllegalArgumentException expected) {
      // success
    }

    // Also try setting the authenticator
    props = new Properties();
    props.setProperty(MCAST_PORT, "26753");
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, LdapUserAuthenticator.class.getName() + ".create");

    try {
      getSystem(props);
      fail("Expected an IllegalArgumentException while connection to DS");

    } catch (IllegalArgumentException expected) {
      // success
    }
  }

  /**
   * AuthInitialize is incorrect
   */
  @Test
  public void testP2PAuthenticationWithInvalidAuthInitialize() throws Exception {
    int locatorPort = getRandomAvailablePort(SOCKET);

    CredentialGenerator gen = new DummyCredentialGenerator();
    assertNotNull(gen.getAuthenticator());
    assertNull(gen.getJavaProperties());

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
    props.setProperty(SECURITY_PEER_AUTH_INIT, "Incorrect_AuthInitialize");
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());

    startTheLocator(props, gen.getJavaProperties(), locatorPort);

    try {
      new SecurityTestUtils("tmp").createSystem(props, null);
      fail("AuthenticationFailedException was expected as the AuthInitialize object passed is incorrect");

    } catch (GemFireSecurityException expected) {
      // success

    } finally {
      locatorVM.invoke(() -> stopLocator(locatorPort, ignoredExceptions));
    }
  }

  /**
   * Authenticator is incorrect
   */
  @Category(FlakyTest.class) // GEODE-1089: random port
  @Test
  public void testP2PAuthenticationWithInvalidAuthenticator() throws Exception {
    int locatorPort = getRandomAvailablePort(SOCKET);

    CredentialGenerator gen = new DummyCredentialGenerator();
    assertNotNull(gen.getAuthInit());
    assertNull(gen.getJavaProperties());

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
    props.setProperty(SECURITY_PEER_AUTH_INIT, gen.getAuthInit());
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, "xyz");

    startTheLocator(props, null, locatorPort);

    try {
      new SecurityTestUtils("tmp").createSystem(props, null);
      fail("AuthenticationFailedException was expected as the Authenticator object passed is incorrect");

    } catch (GemFireSecurityException expected) {
      // success

    } finally {
      locatorVM.invoke(() -> stopLocator(locatorPort, ignoredExceptions));
    }
  }

  @Category(FlakyTest.class) // GEODE-1091: random port
  @Test
  public void testP2PAuthenticationWithNoCredentials() throws Exception {
    int locatorPort = getRandomAvailablePort(SOCKET);

    CredentialGenerator gen = new DummyCredentialGenerator();
    assertNotNull(gen.getAuthenticator());
    assertNotNull(gen.getAuthInit());
    assertNull(gen.getJavaProperties());
    assertNull(gen.getSystemProperties());

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
    props.setProperty(SECURITY_PEER_AUTH_INIT, gen.getAuthInit());
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());

    startTheLocator(props, null, locatorPort);

    try {
      new SecurityTestUtils("tmp").createSystem(props, null);
      fail("AuthenticationFailedException was expected as no credentials are set");

    } catch (GemFireSecurityException expected) {
      // success

    } finally {
      locatorVM.invoke(() -> stopLocator(locatorPort, ignoredExceptions));
    }
  }

  @Test
  public void testP2PAuthenticationWithValidCredentials() throws Exception {
    int locatorPort = getRandomAvailablePort(SOCKET);

    CredentialGenerator gen = new DummyCredentialGenerator();
    assertNotNull(gen.getAuthenticator());
    assertNotNull(gen.getAuthInit());
    assertNull(gen.getJavaProperties());
    assertNull(gen.getSystemProperties());
    assertNotNull(gen.getValidCredentials(1));

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
    props.setProperty(SECURITY_PEER_AUTH_INIT, gen.getAuthInit());
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());
    props.putAll(gen.getValidCredentials(1));

    startTheLocator(props, gen.getJavaProperties(), locatorPort);

    try {
      createDS(props, gen.getJavaProperties());
      verifyMembers(2);
      disconnectFromDS();

    } finally {
      locatorVM.invoke(() -> stopLocator(locatorPort, ignoredExceptions));
    }
  }

  @Test
  public void testP2PAuthenticationWithBothValidAndInValidCredentials() throws Exception {
    addIgnoredException("Authentication failed");

    int locatorPort = getRandomAvailablePort(SOCKET);

    CredentialGenerator gen = new DummyCredentialGenerator();
    assertNotNull(gen.getAuthenticator());
    assertNotNull(gen.getAuthInit());
    assertNotNull(gen.getInvalidCredentials(1));
    assertNull(gen.getJavaProperties());
    assertNull(gen.getSystemProperties());
    assertNotNull(gen.getValidCredentials(1));
    assertNotNull(gen.getValidCredentials(3));

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
    props.setProperty(SECURITY_PEER_AUTH_INIT, gen.getAuthInit());
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());
    props.putAll(gen.getValidCredentials(1));

    startTheLocator(props, null, locatorPort);

    try {
      // invalid credentials for the peer
      props.putAll(gen.getInvalidCredentials(1));

      try {
        new SecurityTestUtils("tmp").createSystem(props, null);
        fail("AuthenticationFailedException was expected as wrong credentials were passed");

      } catch (GemFireSecurityException expected) {
        // success
      }

      props.putAll(gen.getValidCredentials(3));

      createDS(props, null);
      verifyMembers(2);
      disconnectFromDS();

    } finally {
      locatorVM.invoke(() -> stopLocator(locatorPort, ignoredExceptions));
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
  @Ignore("disabled for some reason?")
  @Test
  public void testP2PViewChangeReject() throws Exception {
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
    int port = getRandomAvailablePort(SOCKET);
    final String locators = getIPLiteral() +"["+port+"]";

    props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, authenticator);
    Properties credentials = gen.getValidCredentials(1);
    Properties javaProps = gen.getJavaProperties();
    props.putAll(credentials);
    props.putAll(extraProps);

    startTheLocator(props, javaProps, port);

    try {

      // Start the first peer with different authenticator
      props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
      props.setProperty(SECURITY_PEER_AUTHENTICATOR, authenticator2);

      credentials = gen.getValidCredentials(3);
      Properties javaProps2 = gen2.getJavaProperties();
      props.putAll(credentials);
      props.putAll(extraProps2);

      createDS(props, javaProps2);

      // Start the second peer with the same authenticator as locator
      props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
      props.setProperty(SECURITY_PEER_AUTHENTICATOR, authenticator);

      credentials = gen.getValidCredentials(7);
      javaProps = gen.getJavaProperties();
      props.putAll(credentials);
      props.putAll(extraProps);

      createDS(peer2, props, javaProps);

      createDS(peer3, props, javaProps);

      // wait for view propagation
      pause(2000);

      // Verify the number of members on all peers and locator
      locatorVM.invoke(() -> verifyMembers(4));
      verifyMembers(2);
      peer2.invoke(() -> verifyMembers(4));
      peer3.invoke(() -> verifyMembers(4));

      // Disconnect the first peer and check again
      disconnectFromDS();
      pause(2000);

      locatorVM.invoke(() -> verifyMembers(3));
      peer2.invoke(() -> verifyMembers(3));
      peer3.invoke(() -> verifyMembers(3));

      // Disconnect the second peer and check again
      peer2.invoke(() -> disconnectFromDS());
      pause(2000);

      locatorVM.invoke(() -> verifyMembers(2));
      peer3.invoke(() -> verifyMembers(2));

      // Same for last peer
      peer3.invoke(() -> disconnectFromDS());
      pause(2000);

      locatorVM.invoke(() -> verifyMembers(1));

    } finally {
      locatorVM.invoke(() -> stopLocator(port, ignoredExceptions));
    }
  }

  /**
   * The strategy is to test credential size greater than UDP datagram size.
   * 
   * Here locator will accept the credentials from peer2 and the large credential
   * from the first peer. Number of members in the DS
   * should be four
   */
  @Test
  public void testP2PLargeCredentialSucceeds() throws Exception {
    int locatorPort = getRandomAvailablePort(SOCKET);

    final Host host = Host.getHost(0);
    final VM peer2 = host.getVM(1);
    final VM peer3 = host.getVM(2);

    CredentialGenerator gen = new DummyCredentialGenerator();
    gen.init();

    assertNotNull(gen.getAuthenticator());
    assertNull(gen.getJavaProperties());
    assertNull(gen.getSystemProperties());
    assertNotNull(gen.getValidCredentials(1));

    String authInit = UserPasswordWithExtraPropsAuthInit.class.getName() + ".create";
    Properties credentials = gen.getValidCredentials(1);

    Properties props = new Properties();
    props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());
    props.putAll(credentials);

    startTheLocator(props, null, locatorPort);

    try {
      // Start the first peer with huge credentials
      props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
      props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
      props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());

      String hugeStr = "20KString";
      for (int i = 0; i <= 20000; i++) {
        hugeStr += "A";
      }

      credentials = gen.getValidCredentials(3);
      credentials.setProperty("security-keep-extra-props", "-");
      credentials.setProperty("security-hugeentryone", hugeStr);
      credentials.setProperty("security-hugeentrytwo", hugeStr);
      credentials.setProperty("security-hugeentrythree", hugeStr);

      props.putAll(credentials);

      createDS(props, null);
      // fail("AuthenticationFailedException was expected as credentials were passed beyond 50k"); --?

      // Start the second peer with the same authenticator as locator
      props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, getIPLiteral() + "[" + locatorPort + "]");
      props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
      props.setProperty(SECURITY_PEER_AUTHENTICATOR, gen.getAuthenticator());

      credentials = gen.getValidCredentials(7);
      props.putAll(credentials);

      createDS(peer2, props, null);
      createDS(peer3, props, null);

      // wait for view propagation
      pause(2000);

      // Verify the number of members on all peers and locator
      locatorVM.invoke(() -> verifyMembers(4));
      peer2.invoke(() -> verifyMembers(4));
      peer3.invoke(() -> verifyMembers(4));

      // Disconnect the peers
      disconnectFromDS();
      peer2.invoke(() -> disconnectFromDS());
      peer3.invoke(() -> disconnectFromDS());

    } finally {
      locatorVM.invoke(() -> stopLocator(locatorPort, ignoredExceptions));
    }
  }

  private void createDS(final VM peer2, final Properties props, final Properties javaProps) {
    peer2.invoke(() -> createDS(props, javaProps));
  }

  private void startTheLocator(final Properties props, final Properties javaProps, final int port) {
    locatorVM.invoke(() -> startLocator(getUniqueName(), port, props, javaProps, ignoredExceptions));
  }

  private static void createDS(final Properties props, final Properties javaProps) {
    SecurityTestUtils tmpUtil = new SecurityTestUtils("tmp");
    tmpUtil.createSystem(props, javaProps);
  }

  private static void verifyMembers(final int numExpectedMembers) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    MembershipManager mgr = MembershipManagerHelper.getMembershipManager(ds);
    assertEquals(numExpectedMembers, mgr.getView().size());
  }
}
