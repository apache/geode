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

import static org.apache.geode.cache.Region.SEPARATOR;
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
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.internal.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityTestUtils;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({WanTest.class})
public class NewWanAuthenticationDUnitTest extends WANTestBase {

  private static final Logger logger = LogService.getLogger();

  private static boolean isDifferentServerInGetCredentialCall = false;

  private static final String securityJsonResource =
      "org/apache/geode/security/templates/security.json";
  private static final String senderId = "ln";
  private static final int numPuts = 10;

  private Integer lnPort;
  private Integer nyPort;
  private String regionName;
  private final VM sender = vm2;
  private final VM receiver = vm3;

  @Before
  public void setup() {
    disconnectAllFromDS();
    lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    regionName = getTestMethodName() + "_RR";
  }

  /**
   * Authentication test for new WAN with valid credentials. Although, nothing related to
   * authentication has been changed in new WAN, this test case is added on request from QA for
   * defect 44650.
   */
  @Test
  public void testWanAuthValidCredentials() {
    final CredentialGenerator credentialGenerator = new DummyCredentialGenerator();

    final Properties extraProps = credentialGenerator.getSystemProperties();

    final Properties senderCredentials = credentialGenerator.getValidCredentials(1);
    if (extraProps != null) {
      senderCredentials.putAll(extraProps);
    }
    final Properties senderJavaProps = credentialGenerator.getJavaProperties();

    // receiver's invalid credentials
    final Properties receiverCredentials = credentialGenerator.getInvalidCredentials(1);
    if (extraProps != null) {
      receiverCredentials.putAll(extraProps);
    }
    final Properties receiverJavaProps = credentialGenerator.getJavaProperties();

    final String clientAuthenticator = credentialGenerator.getAuthenticator();
    final String clientAuthInit = credentialGenerator.getAuthInit();

    final Properties senderSecurityProps =
        buildProperties(clientAuthenticator, clientAuthInit, null, senderCredentials, null);
    // have receiver start a cache with invalid credentials
    final Properties receiverSecurityProps =
        buildProperties(clientAuthenticator, clientAuthInit, null, receiverCredentials, null);

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    sender.invoke(() -> createSecuredCache(senderSecurityProps, senderJavaProps, lnPort));
    receiver.invoke(() -> createSecuredCache(receiverSecurityProps, receiverJavaProps, nyPort));

    sender.invoke(() -> createSender("ln", 2, false, 100, 10, false, false, null, true));
    receiver.invoke(() -> createReceiverInSecuredCache());

    sender.invoke(
        () -> createReplicatedRegion(regionName, "ln", isOffHeap()));
    receiver.invoke(
        () -> createReplicatedRegion(regionName, null, isOffHeap()));

    // this tests verifies that even though the receiver has invalid credentials, the sender can
    // still send data to
    // the receiver because the sender has valid credentials
    sender.invoke(() -> startSender("ln"));
    sender.invoke(() -> waitForSenderRunningState("ln"));

    sender.invoke(() -> doPuts(regionName, 1));
    receiver.invoke(() -> {
      Region r = cache.getRegion(SEPARATOR + regionName);
      await().untilAsserted(() -> assertTrue(r.size() > 0));
    });
  }

  @Test
  public void testWanIntegratedSecurityWithValidCredentials() {
    final Properties senderSecurityProps = buildSecurityProperties("admin", "secret");
    final Properties receiverSecurityProps = buildSecurityProperties("guest", "guest");

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    sender.invoke(() -> createSecuredCache(senderSecurityProps, null, lnPort));
    receiver.invoke(() -> createSecuredCache(receiverSecurityProps, null, nyPort));

    sender.invoke(() -> createSender("ln", 2, false, 100, 10, false, false, null, true));
    receiver.invoke(() -> createReceiverInSecuredCache());

    sender.invoke(
        () -> createReplicatedRegion(regionName, "ln", isOffHeap()));
    receiver.invoke(
        () -> createReplicatedRegion(regionName, null, isOffHeap()));

    sender.invoke(() -> startSender("ln"));
    sender.invoke(() -> waitForSenderRunningState("ln"));
    sender.invoke(() -> doPuts(regionName, 1));

    receiver.invoke(() -> {
      Region r = cache.getRegion(SEPARATOR + regionName);
      await().untilAsserted(() -> assertTrue(r.size() > 0));
    });
  }

  /**
   * Test authentication with new WAN with invalid credentials. Although, nothing related to
   * authentication has been changed in new WAN, this test case is added on request from QA for
   * defect 44650.
   */
  @Test
  public void testWanAuthInvalidCredentials() {
    final CredentialGenerator credentialGenerator = new DummyCredentialGenerator();

    final Properties extraProps = credentialGenerator.getSystemProperties();

    final Properties senderCredentials = credentialGenerator.getInvalidCredentials(1);
    if (extraProps != null) {
      senderCredentials.putAll(extraProps);
    }
    final Properties senderJavaProperties = credentialGenerator.getJavaProperties();

    final Properties receiverCredentials = credentialGenerator.getInvalidCredentials(2);
    if (extraProps != null) {
      receiverCredentials.putAll(extraProps);
    }
    final Properties receiverJavaProps = credentialGenerator.getJavaProperties();

    final String clientAuthenticator = credentialGenerator.getAuthenticator();
    final String clientAuthInit = credentialGenerator.getAuthInit();

    final Properties senderSecurityProps =
        buildProperties(clientAuthenticator, clientAuthInit, null, senderCredentials, null);
    final Properties receiverSecurityPropsWithIncorrectSenderCreds =
        buildProperties(clientAuthenticator, clientAuthInit, null, receiverCredentials, null);

    // ------------------------------ Set Up With Invalid Credentials ------------------------------

    sender.invoke(() -> {
      createSecuredCache(senderSecurityProps, senderJavaProperties, lnPort);
      createReplicatedRegion(regionName, senderId, isOffHeap());
      createSender(senderId, 2, false, 100, 10, false, false, null, true);
    });

    receiver.invoke(() -> {
      createSecuredReceiver(nyPort, regionName, receiverSecurityPropsWithIncorrectSenderCreds,
          receiverJavaProps);
    });

    sender.invoke(() -> {
      startSender(senderId);
      doPutsAndVerifyQueueSizeAfterProcessing(regionName, numPuts, false, true, false);
    });

    receiver.invoke(() -> validateRegionSize(regionName, 0));
  }

  /**
   * Test authentication with new WAN with invalid credentials. Although, nothing related to
   * authentication has been changed in new WAN, this test case is added on request from QA for
   * defect 44650.
   */
  @Test
  public void testWanSecurityManagerWithInvalidThenValidCredentials() {
    final Properties senderSecurityProps = buildSecurityProperties("admin", "wrongPswd");

    final String securityJsonResource =
        "org/apache/geode/internal/cache/wan/misc/NewWanAuthenticationDUnitTest.testWanSecurityManagerWithInvalidCredentials.security.json";
    final Properties receiverSecurityPropsWithCorrectSenderCreds =
        buildSecurityProperties(securityJsonResource);
    final Properties receiverSecurityPropsWithIncorrectSenderCreds = buildSecurityProperties();

    // ------------------------------ Set Up With Invalid Credentials ------------------------------

    sender.invoke(() -> {
      createSecuredCache(senderSecurityProps, null, lnPort);
      createReplicatedRegion(regionName, senderId, isOffHeap());
      createSender(senderId, 2, false, 100, 10, false, false, null, true);
    });

    receiver.invoke(() -> {
      createSecuredReceiver(nyPort, regionName, receiverSecurityPropsWithIncorrectSenderCreds,
          null);
    });

    sender.invoke(() -> {
      startSender(senderId);
      doPutsAndVerifyQueueSizeAfterProcessing(regionName, numPuts, false, true, false);
    });

    receiver.invoke(() -> validateRegionSize(regionName, 0));

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    receiver.invoke(() -> {
      closeCache();
      createSecuredReceiver(nyPort, regionName, receiverSecurityPropsWithCorrectSenderCreds, null);
    });

    sender.invoke(() -> {
      doPutsAndVerifyQueueSizeAfterProcessing(regionName, numPuts, true, false, true);
    });

    receiver.invoke(() -> validateRegionSize(regionName, numPuts));
  }

  @Test
  public void testWanSecurityManagerWithValidThenInvalidThenValidCredentials() {
    final String securityJsonResource =
        "org/apache/geode/internal/cache/wan/misc/NewWanAuthenticationDUnitTest.testWanSecurityManagerWithInvalidCredentials.security.json";
    final String gatewayConnectionRetryIntervalConfigParameter =
        GeodeGlossary.GEMFIRE_PREFIX + "gateway-connection-retry-interval";

    final Properties senderSecurityProps = buildSecurityProperties("admin", "wrongPswd");

    final Properties receiverSecurityPropsWithCorrectSenderCreds =
        buildSecurityProperties(securityJsonResource);
    final Properties receiverSecurityPropsWithIncorrectSenderCreds = buildSecurityProperties();

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    sender.invoke(() -> {
      createSecuredCache(senderSecurityProps, null, lnPort);
      createReplicatedRegion(regionName, senderId, isOffHeap());
      createSender(senderId, 2, false, 100, 10, false, false, null, true);
    });

    receiver.invoke(() -> {
      createSecuredReceiver(nyPort, regionName, receiverSecurityPropsWithCorrectSenderCreds, null);
    });

    sender.invoke(() -> {
      startSender(senderId);
      doPutsAndVerifyQueueSizeAfterProcessing(regionName, numPuts, true, false, true);
    });

    receiver.invoke(() -> validateRegionSize(regionName, numPuts));

    // ------------------------------ Set Up With Invalid Credentials ------------------------------

    receiver.invoke(() -> {
      // Simulate restarting the receiver, this time without valid credentials for the sender.
      closeCache();
      createSecuredReceiver(nyPort, regionName, receiverSecurityPropsWithIncorrectSenderCreds,
          null);
    });

    sender.invoke(() -> {
      doPutsAndVerifyQueueSizeAfterProcessing(regionName, numPuts, false, true, true);
    });

    receiver.invoke(() -> validateRegionSize(regionName, 0));

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    receiver.invoke(() -> {
      closeCache();
      // Simulate restarting the receiver, and restore valid credentials for the sender.
      createSecuredReceiver(nyPort, regionName, receiverSecurityPropsWithCorrectSenderCreds, null);
    });

    sender.invoke(() -> {
      /*
       * Data should be able to flow properly after valid credentials have been restored.
       * No more puts are needed because we already have numPuts queued from when credentials
       * were invalid (see above).
       */
      doPutsAndVerifyQueueSizeAfterProcessing(regionName, 0, true, false, true);
    });

    receiver.invoke(() -> validateRegionSize(regionName, numPuts));
  }

  @Test
  public void testWanAuthValidCredentialsWithServer() {
    final DummyCredentialGenerator credentialGenerator = new DummyCredentialGenerator();
    credentialGenerator.init();

    final Properties extraProps = credentialGenerator.getSystemProperties();

    final Properties senderCredentials = credentialGenerator.getValidCredentials(1);
    if (extraProps != null) {
      senderCredentials.putAll(extraProps);
    }
    final Properties senderJavaProps = credentialGenerator.getJavaProperties();

    final Properties receiverCredentials = credentialGenerator.getInvalidCredentials(2);
    if (extraProps != null) {
      receiverCredentials.putAll(extraProps);
    }
    final Properties receiverJavaProps = credentialGenerator.getJavaProperties();

    final String clientAuthenticator = credentialGenerator.getAuthenticator();
    final String clientAuthInit = UserPasswdAI.class.getName() + ".createAI";

    final Properties senderSecurityProps =
        buildProperties(clientAuthenticator, clientAuthInit, null, senderCredentials, null);
    final Properties receiverSecurityProps =
        buildProperties(clientAuthenticator, clientAuthInit, null, receiverCredentials, null);

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    sender.invoke(() -> createSecuredCache(senderSecurityProps, senderJavaProps, lnPort));

    receiver.invoke(() -> createSecuredCache(receiverSecurityProps, receiverJavaProps, nyPort));

    sender.invoke(() -> createSender("ln", 2, false, 100, 10, false, false, null, true));

    receiver.invoke(() -> createReceiverInSecuredCache());

    sender.invoke(() -> {
      startSender("ln");
      waitForSenderRunningState("ln");
      verifyDifferentServerInGetCredentialCall();
    });

    receiver.invoke(() -> verifyDifferentServerInGetCredentialCall());
  }

  @Test
  public void testWanSecurityManagerAuthValidCredentialsWithServer() {
    Properties senderSecurityProps = buildSecurityProperties("admin", "secret");
    Properties receiverSecurityProps = buildSecurityProperties("guest", "guest");

    // ------------------------------- Set Up With Valid Credentials -------------------------------

    sender.invoke(() -> createSecuredCache(senderSecurityProps, null, lnPort));

    receiver.invoke(() -> createSecuredCache(receiverSecurityProps, null, nyPort));

    sender.invoke(() -> createSender("ln", 2, false, 100, 10, false, false, null, true));

    receiver.invoke(() -> createReceiverInSecuredCache());

    sender.invoke(() -> {
      startSender("ln");
      waitForSenderRunningState("ln");
      verifyDifferentServerInGetCredentialCall();
    });

    // this would fail for now because for integrated security, we are not sending the receiver's
    // credentials back
    // to the sender. Because in the old security implementation, even though the receiver's
    // credentials are sent back to the sender
    // the sender is not checking it.
    // sender.invoke(() -> verifyDifferentServerInGetCredentialCall());
  }

  private void doPutsAndVerifyQueueSizeAfterProcessing(
      final String regionName,
      final int numPuts,
      final boolean shouldBeConnected,
      final boolean isQueueBlocked,
      final boolean isAckThreadRunning) {
    if (isQueueBlocked) {
      // caller is assuming that queue processing will not make progress
      try {
        final LongAdder dispatchAttempts = new LongAdder();
        final LongAdder ackReadAttempts = new LongAdder();

        GatewaySenderEventRemoteDispatcher.messageProcessingAttempted = isAck -> {
          if (isAck) {
            ackReadAttempts.increment();
          } else {
            dispatchAttempts.increment();
          }
        };

        doPuts(regionName, numPuts);

        /*
         * The game here is to ensure that both the dispatcher thread and the ack reader thread
         * each get at least one whack at processing (a batch or an ack, respectively).
         * Note: both those conditions aren't obviously necessary from our method signature, but
         * trust us: callers rely on that guarantee! (the "Processing" in the method name
         * implies that _both_ threads tried).
         *
         * Notice the particular awfulness of the second term in the conditional below. Callers
         * have to send us a flag to tell us that the ack reader thread is not running so we know
         * not to look for its attempts.
         */
        await().until(() -> dispatchAttempts.sum() > 0 &&
            (!isAckThreadRunning || ackReadAttempts.sum() > 0));

        checkQueueSize(senderId, numPuts);
      } finally {
        GatewaySenderEventRemoteDispatcher.messageProcessingAttempted = isAck -> {
        };
      }
    } else {
      doPuts(regionName, numPuts);
      // caller is assuming queue will drain eventually
      checkQueueSize(senderId, 0);
    }
    verifyRunningWithConnectedState(senderId, shouldBeConnected);
  }

  private void createSecuredReceiver(Integer nyPort, String regionName,
      Properties receiverSecurityPropsWithCorrectSenderCreds,
      Object javaProps) {
    createSecuredCache(receiverSecurityPropsWithCorrectSenderCreds, javaProps, nyPort);
    createReplicatedRegion(regionName, null, isOffHeap());
    createReceiverInSecuredCache();
  }

  private void verifyRunningWithConnectedState(
      final String senderId,
      final boolean shouldBeConnected) {
    await().untilAsserted(() -> {
      verifySenderRunningState(senderId);
      verifySenderConnectedState(senderId, shouldBeConnected);
    });
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

  private static Properties buildSecurityProperties(final String username, final String password) {
    final Properties props = buildSecurityProperties();
    props.put("security-username", username);
    props.put("security-password", password);
    return props;
  }

  private static Properties buildSecurityProperties(
      final String securityJsonResource) {
    final Properties props = buildSecurityProperties();
    props.put("security-json", securityJsonResource);
    return props;
  }

  private static Properties buildSecurityProperties() {
    final Properties props = new Properties();
    props.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
    props.put(SECURITY_CLIENT_AUTH_INIT, UserPasswdAI.class.getName());
    props.put("security-json", securityJsonResource);
    return props;
  }

  private static void createSecuredCache(Properties authProps, Object javaProps, Integer locPort) {
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

  private static void verifyDifferentServerInGetCredentialCall() {
    Assert.assertTrue(isDifferentServerInGetCredentialCall,
        "verifyDifferentServerInGetCredentialCall: Server should be different");
    isDifferentServerInGetCredentialCall = false;
  }
}
