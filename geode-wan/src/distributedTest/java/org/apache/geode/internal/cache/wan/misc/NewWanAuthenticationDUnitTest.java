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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityTestUtils;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class NewWanAuthenticationDUnitTest extends WANTestBase {

  public static final Logger logger = LogService.getLogger();

  public static boolean isDifferentServerInGetCredentialCall = false;

  /**
   * Authentication test for new WAN with valid credentials. Although, nothing related to
   * authentication has been changed in new WAN, this test case is added on request from QA for
   * defect 44650.
   */
  @Test
  public void testWanAuthValidCredentials() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    logger.info("Created locator on local site");

    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    logger.info("Created locator on remote site");


    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();

    String clientauthenticator = gen.getAuthenticator();
    String clientauthInit = gen.getAuthInit();

    Properties credentials1 = gen.getValidCredentials(1);
    if (extraProps != null) {
      credentials1.putAll(extraProps);
    }
    Properties javaProps1 = gen.getJavaProperties();

    // vm3's invalid credentials
    Properties credentials2 = gen.getInvalidCredentials(1);
    if (extraProps != null) {
      credentials2.putAll(extraProps);
    }
    Properties javaProps2 = gen.getJavaProperties();

    Properties props1 =
        buildProperties(clientauthenticator, clientauthInit, null, credentials1, null);

    // have vm 3 start a cache with invalid credentails
    Properties props2 =
        buildProperties(clientauthenticator, clientauthInit, null, credentials2, null);

    vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props1, javaProps1, lnPort));
    logger.info("Created secured cache in vm2");

    vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props2, javaProps2, nyPort));
    logger.info("Created secured cache in vm3");

    vm2.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    logger.info("Created sender in vm2");

    vm3.invoke(() -> createReceiverInSecuredCache());
    logger.info("Created receiver in vm3");

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    logger.info("Created RR in vm2");
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    logger.info("Created RR in vm3");

    // this tests verifies that even though vm3 has invalid credentials, vm2 can still send data to
    // vm3 because
    // vm2 has valid credentials
    vm2.invoke(() -> WANTestBase.startSender("ln"));
    vm2.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm2.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1));
    vm3.invoke(() -> {
      Region r = cache.getRegion(Region.SEPARATOR + getTestMethodName() + "_RR");
      await().untilAsserted(() -> assertTrue(r.size() > 0));
    });
    logger.info("Done successfully.");
  }

  @Test
  public void testWanIntegratedSecurityWithValidCredentials() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    logger.info("Created locator on local site");

    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    logger.info("Created locator on remote site");


    Properties props1 = buildSecurityProperties("admin", "secret");
    Properties props2 = buildSecurityProperties("guest", "guest");

    vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props1, null, lnPort));
    logger.info("Created secured cache in vm2");

    vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props2, null, nyPort));
    logger.info("Created secured cache in vm3");

    vm2.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    logger.info("Created sender in vm2");

    vm3.invoke(() -> createReceiverInSecuredCache());
    logger.info("Created receiver in vm3");

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    logger.info("Created RR in vm2");
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    logger.info("Created RR in vm3");

    vm2.invoke(() -> WANTestBase.startSender("ln"));
    vm2.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm2.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1));
    vm3.invoke(() -> {
      Region r = cache.getRegion(Region.SEPARATOR + getTestMethodName() + "_RR");
      await().untilAsserted(() -> assertTrue(r.size() > 0));

    });
    logger.info("Done successfully.");
  }

  /**
   * Test authentication with new WAN with invalid credentials. Although, nothing related to
   * authentication has been changed in new WAN, this test case is added on request from QA for
   * defect 44650.
   */
  @Test
  public void testWanAuthInvalidCredentials() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    logger.info("Created locator on local site");

    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    logger.info("Created locator on remote site");


    CredentialGenerator gen = new DummyCredentialGenerator();
    logger.info("Picked up credential: " + gen);

    Properties extraProps = gen.getSystemProperties();

    String clientauthenticator = gen.getAuthenticator();
    String clientauthInit = gen.getAuthInit();

    Properties credentials1 = gen.getInvalidCredentials(1);
    if (extraProps != null) {
      credentials1.putAll(extraProps);
    }
    Properties javaProps1 = gen.getJavaProperties();
    Properties credentials2 = gen.getInvalidCredentials(2);
    if (extraProps != null) {
      credentials2.putAll(extraProps);
    }
    Properties javaProps2 = gen.getJavaProperties();

    Properties props1 =
        buildProperties(clientauthenticator, clientauthInit, null, credentials1, null);
    Properties props2 =
        buildProperties(clientauthenticator, clientauthInit, null, credentials2, null);

    logger.info("Done building auth properties");

    vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props1, javaProps1, lnPort));
    logger.info("Created secured cache in vm2");

    vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props2, javaProps2, nyPort));
    logger.info("Created secured cache in vm3");

    vm2.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    logger.info("Created sender in vm2");

    vm3.invoke(() -> createReceiverInSecuredCache());
    logger.info("Created receiver in vm3");

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    logger.info("Created RR in vm2");
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    logger.info("Created RR in vm3");

    // Start sender
    vm2.invoke(() -> WANTestBase.startSender("ln"));

    // Verify the sender is started
    vm2.invoke(() -> verifySenderRunningState("ln"));

    // Verify the sender is not connected
    vm2.invoke(() -> verifySenderConnectedState("ln", false));
  }

  /**
   * Test authentication with new WAN with invalid credentials. Although, nothing related to
   * authentication has been changed in new WAN, this test case is added on request from QA for
   * defect 44650.
   */
  @Test
  public void testWanSecurityManagerWithInvalidCredentials() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    logger.info("Created locator on local site");

    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    logger.info("Created locator on remote site");

    Properties props1 = buildSecurityProperties("admin", "wrongPswd");
    Properties props2 = buildSecurityProperties("guest", "wrongPswd");

    logger.info("Done building auth properties");

    vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props1, null, lnPort));
    logger.info("Created secured cache in vm2");

    vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props2, null, nyPort));
    logger.info("Created secured cache in vm3");

    String senderId = "ln";
    vm2.invoke(
        () -> WANTestBase.createSender(senderId, 2, false, 100, 10, false, false, null, true));
    logger.info("Created sender in vm2");

    vm3.invoke(() -> createReceiverInSecuredCache());
    logger.info("Created receiver in vm3");

    String regionName = getTestMethodName() + "_RR";
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(regionName, senderId, isOffHeap()));
    logger.info("Created RR in vm2");
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(regionName, null, isOffHeap()));
    logger.info("Created RR in vm3");

    // Start sender
    vm2.invoke(() -> WANTestBase.startSender(senderId));

    // Verify the sender is started
    vm2.invoke(() -> verifySenderRunningState(senderId));

    // Verify the sender is not connected
    vm2.invoke(() -> verifySenderConnectedState(senderId, false));

    // Do some puts in the sender
    int numPuts = 10;
    vm2.invoke(() -> WANTestBase.doPuts(regionName, numPuts));

    // Verify the sender is still started
    vm2.invoke(() -> verifySenderRunningState(senderId));

    // Verify the sender is still not connected
    vm2.invoke(() -> verifySenderConnectedState(senderId, false));

    // Verify the sender queue size
    vm2.invoke(() -> testQueueSize(senderId, numPuts));

    // Stop the receiver
    vm3.invoke(() -> closeCache());

    // Restart the receiver with a SecurityManager that accepts the existing sender's username and
    // password. The
    // NewWanAuthenticationDUnitTest.testWanSecurityManagerWithInvalidCredentials.security.json.
    // file contains the admin user definition that the SecurityManager will accept.
    String securityJsonRersource = "org/apache/geode/internal/cache/wan/misc/"
        + getClass().getSimpleName() + "." + getTestMethodName() + ".security.json";
    Properties propsRestart = buildSecurityProperties("guest", "guest", securityJsonRersource);
    vm3.invoke(() -> createSecuredCache(propsRestart, null, nyPort));
    vm3.invoke(() -> createReplicatedRegion(regionName, null, isOffHeap()));
    vm3.invoke(() -> createReceiverInSecuredCache());

    // Wait for the queue to drain
    vm2.invoke(() -> checkQueueSize(senderId, 0));

    // Verify region size on receiver
    vm3.invoke(() -> validateRegionSize(regionName, numPuts));
  }

  private static Properties buildProperties(String clientauthenticator, String clientAuthInit,
      String accessor, Properties extraAuthProps, Properties extraAuthzProps) {
    Properties authProps = new Properties();
    if (clientauthenticator != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR, clientauthenticator);
    }
    if (accessor != null) {
      authProps.setProperty(SECURITY_CLIENT_ACCESSOR, accessor);
    }
    if (clientAuthInit != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTH_INIT, clientAuthInit);
    }
    if (extraAuthProps != null) {
      authProps.putAll(extraAuthProps);
    }
    if (extraAuthzProps != null) {
      authProps.putAll(extraAuthzProps);
    }
    return authProps;
  }

  private static Properties buildSecurityProperties(String username, String password) {
    return buildSecurityProperties(username, password,
        "org/apache/geode/security/templates/security.json");
  }

  private static Properties buildSecurityProperties(String username, String password,
      String securityJsonResource) {
    Properties props = new Properties();
    props.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
    props.put("security-json", securityJsonResource);
    props.put(SECURITY_CLIENT_AUTH_INIT, UserPasswdAI.class.getName());
    props.put("security-username", username);
    props.put("security-password", password);
    return props;
  }

  public static void createSecuredCache(Properties authProps, Object javaProps, Integer locPort) {
    authProps.setProperty(MCAST_PORT, "0");
    authProps.setProperty(LOCATORS, "localhost[" + locPort + "]");

    logger.info("Set the server properties to: " + authProps);
    logger.info("Set the java properties to: " + javaProps);

    SecurityTestUtils tmpInstance = new SecurityTestUtils();
    DistributedSystem ds = tmpInstance.createSystem(authProps, (Properties) javaProps);
    assertNotNull(ds);
    assertTrue(ds.isConnected());
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static class UserPasswdAI extends UserPasswordAuthInit {

    public static AuthInitialize createAI() {
      return new UserPasswdAI();
    }

    @Override
    public Properties getCredentials(Properties props, DistributedMember server, boolean isPeer)
        throws AuthenticationFailedException {
      boolean val = (CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember()
          .getProcessId() != server.getProcessId());
      Assert.assertTrue(val, "getCredentials: Server should be different");
      Properties p = super.getCredentials(props, server, isPeer);
      if (val) {
        isDifferentServerInGetCredentialCall = true;
        CacheFactory.getAnyInstance().getLogger()
            .config("setting  isDifferentServerInGetCredentialCall "
                + isDifferentServerInGetCredentialCall);
      } else {
        CacheFactory.getAnyInstance().getLogger()
            .config("setting22  isDifferentServerInGetCredentialCall "
                + isDifferentServerInGetCredentialCall);
      }
      return p;
    }
  }

  public static void verifyDifferentServerInGetCredentialCall() {
    Assert.assertTrue(isDifferentServerInGetCredentialCall,
        "verifyDifferentServerInGetCredentialCall: Server should be different");
    isDifferentServerInGetCredentialCall = false;
  }

  @Test
  public void testWanAuthValidCredentialsWithServer() {
    disconnectAllFromDS();
    {
      Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      logger.info("Created locator on local site");

      Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
      logger.info("Created locator on remote site");

      DummyCredentialGenerator gen = new DummyCredentialGenerator();
      gen.init();
      Properties extraProps = gen.getSystemProperties();

      String clientauthenticator = gen.getAuthenticator();
      String clientauthInit = UserPasswdAI.class.getName() + ".createAI";

      Properties credentials1 = gen.getValidCredentials(1);
      if (extraProps != null) {
        credentials1.putAll(extraProps);
      }
      Properties javaProps1 = gen.getJavaProperties();

      Properties credentials2 = gen.getInvalidCredentials(2);
      if (extraProps != null) {
        credentials2.putAll(extraProps);
      }
      Properties javaProps2 = gen.getJavaProperties();

      Properties props1 =
          buildProperties(clientauthenticator, clientauthInit, null, credentials1, null);
      Properties props2 =
          buildProperties(clientauthenticator, clientauthInit, null, credentials2, null);

      vm2.invoke(
          () -> NewWanAuthenticationDUnitTest.createSecuredCache(props1, javaProps1, lnPort));
      logger.info("Created secured cache in vm2");

      vm3.invoke(
          () -> NewWanAuthenticationDUnitTest.createSecuredCache(props2, javaProps2, nyPort));
      logger.info("Created secured cache in vm3");

      vm2.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
      logger.info("Created sender in vm2");

      vm3.invoke(() -> createReceiverInSecuredCache());
      logger.info("Created receiver in vm3");

      vm2.invoke(() -> WANTestBase.startSender("ln"));
      vm2.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

      vm2.invoke(() -> verifyDifferentServerInGetCredentialCall());
      vm3.invoke(() -> verifyDifferentServerInGetCredentialCall());
    }
  }

  @Test
  public void testWanSecurityManagerAuthValidCredentialsWithServer() {
    disconnectAllFromDS();
    {
      Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      logger.info("Created locator on local site");

      Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
      logger.info("Created locator on remote site");

      Properties props1 = buildSecurityProperties("admin", "secret");
      Properties props2 = buildSecurityProperties("guest", "guest");

      vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props1, null, lnPort));
      logger.info("Created secured cache in vm2");

      vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(props2, null, nyPort));
      logger.info("Created secured cache in vm3");

      vm2.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
      logger.info("Created sender in vm2");

      vm3.invoke(() -> createReceiverInSecuredCache());
      logger.info("Created receiver in vm3");

      vm2.invoke(() -> WANTestBase.startSender("ln"));
      vm2.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

      vm2.invoke(() -> verifyDifferentServerInGetCredentialCall());

      // this would fail for now because for integrated security, we are not sending the receiver's
      // credentials back
      // to the sender. Because in the old security implementation, even though the receiver's
      // credentials are sent back to the sender
      // the sender is not checking it.
      // vm3.invoke(() -> verifyDifferentServerInGetCredentialCall());
    }
  }
}
