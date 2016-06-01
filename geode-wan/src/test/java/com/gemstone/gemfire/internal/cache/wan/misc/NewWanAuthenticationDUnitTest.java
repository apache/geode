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
package com.gemstone.gemfire.internal.cache.wan.misc;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.SecurityTestUtils;
import com.gemstone.gemfire.security.generator.CredentialGenerator;
import com.gemstone.gemfire.security.generator.DummyCredentialGenerator;
import com.gemstone.gemfire.security.templates.UserPasswordAuthInit;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

public class NewWanAuthenticationDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public static final Logger logger = LogService.getLogger();

  public NewWanAuthenticationDUnitTest(String name) {
    super(name);
  }

  /**
   * Authentication test for new WAN with valid credentials. Although, nothing
   * related to authentication has been changed in new WAN, this test case is
   * added on request from QA for defect 44650.
   */
  public void testWanAuthValidCredentials() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    logger.info("Created locator on local site");

    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
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

    Properties credentials2 = gen.getValidCredentials(2);
    if (extraProps != null) {
      credentials2.putAll(extraProps);
    }
    Properties javaProps2 = gen.getJavaProperties();

    Properties props1 = buildProperties(clientauthenticator, clientauthInit,
      null, credentials1, null);
    Properties props2 = buildProperties(clientauthenticator, clientauthInit,
      null, credentials2, null);

    vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(
      props1, javaProps1, lnPort ));
    logger.info("Created secured cache in vm2");

    vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(
      props2, javaProps2, nyPort ));
    logger.info("Created secured cache in vm3");

    vm2.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    logger.info("Created sender in vm2");

    vm3.invoke(() -> createReceiverInSecuredCache());
    logger.info("Created receiver in vm3");

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    logger.info("Created RR in vm2");
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap()  ));
    logger.info("Created RR in vm3");

    vm2.invoke(() -> WANTestBase.startSender( "ln" ));
    vm2.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    logger.info("Done successfully.");

  }

  /**
   * Test authentication with new WAN with invalid credentials. Although,
   * nothing related to authentication has been changed in new WAN, this test
   * case is added on request from QA for defect 44650.
   */
  public void testWanAuthInvalidCredentials() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    logger.info("Created locator on local site");

    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
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

    Properties props1 = buildProperties(clientauthenticator, clientauthInit,
      null, credentials1, null);
    Properties props2 = buildProperties(clientauthenticator, clientauthInit,
      null, credentials2, null);

    logger.info("Done building auth properties");

    vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(
      props1, javaProps1, lnPort ));
    logger.info("Created secured cache in vm2");

    vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(
      props2, javaProps2, nyPort ));
    logger.info("Created secured cache in vm3");

    vm2.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    logger.info("Created sender in vm2");

    vm3.invoke(() -> createReceiverInSecuredCache());
    logger.info("Created receiver in vm3");

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    logger.info("Created RR in vm2");
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap()  ));
    logger.info("Created RR in vm3");

    try {
      vm2.invoke(() -> WANTestBase.startSender( "ln" ));
      fail("Authentication Failed: While starting the sender, an exception should have been thrown");
    } catch (Exception e) {
      if (!(e.getCause().getCause() instanceof AuthenticationFailedException)) {
        fail("Authentication is not working as expected");
      }
    }
  }

  private static Properties buildProperties(String clientauthenticator,
                                            String clientAuthInit, String accessor, Properties extraAuthProps,
                                            Properties extraAuthzProps) {

    Properties authProps = new Properties();
    if (clientauthenticator != null) {
      authProps.setProperty(
        SECURITY_CLIENT_AUTHENTICATOR,
        clientauthenticator);
    }
    if (accessor != null) {
      authProps.setProperty(SECURITY_CLIENT_ACCESSOR,
        accessor);
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

  public static void createSecuredCache(Properties authProps, Object javaProps, Integer locPort) {
    authProps.setProperty(MCAST_PORT, "0");
    authProps.setProperty(LOCATORS, "localhost[" + locPort + "]");

    logger.info("Set the server properties to: " + authProps);
    logger.info("Set the java properties to: " + javaProps);

    SecurityTestUtils tmpInstance = new SecurityTestUtils("temp");
    DistributedSystem ds = tmpInstance.createSystem(authProps, (Properties)javaProps);
    assertNotNull(ds);
    assertTrue(ds.isConnected());
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static boolean isDifferentServerInGetCredentialCall = false;
  public static class UserPasswdAI extends UserPasswordAuthInit {
    public static AuthInitialize createAI() {
      return new UserPasswdAI();
    }
    @Override
    public Properties getCredentials(Properties props,
                                     DistributedMember server, boolean isPeer)
      throws AuthenticationFailedException {
      boolean val = ( CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember().getProcessId() != server.getProcessId());
      Assert.assertTrue(val, "getCredentials: Server should be different");
      Properties p = super.getCredentials(props, server, isPeer);
      if(val) {
        isDifferentServerInGetCredentialCall = true;
        CacheFactory.getAnyInstance().getLoggerI18n().convertToLogWriter().config("setting  isDifferentServerInGetCredentialCall " + isDifferentServerInGetCredentialCall);
      } else {
        CacheFactory.getAnyInstance().getLoggerI18n().convertToLogWriter().config("setting22  isDifferentServerInGetCredentialCall " + isDifferentServerInGetCredentialCall);
      }
      return p;
    }
  }

  public static void verifyDifferentServerInGetCredentialCall(){
    Assert.assertTrue(isDifferentServerInGetCredentialCall, "verifyDifferentServerInGetCredentialCall: Server should be different");
    isDifferentServerInGetCredentialCall = false;
  }

  public void testWanAuthValidCredentialsWithServer() {
    disconnectAllFromDS();
    {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
      logger.info("Created locator on local site");

      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
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

      Properties credentials2 = gen.getValidCredentials(2);
      if (extraProps != null) {
        credentials2.putAll(extraProps);
      }
      Properties javaProps2 = gen.getJavaProperties();

      Properties props1 = buildProperties(clientauthenticator, clientauthInit,
        null, credentials1, null);
      Properties props2 = buildProperties(clientauthenticator, clientauthInit,
        null, credentials2, null);

      vm2.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(
        props1, javaProps1, lnPort ));
      logger.info("Created secured cache in vm2");

      vm3.invoke(() -> NewWanAuthenticationDUnitTest.createSecuredCache(
        props2, javaProps2, nyPort ));
      logger.info("Created secured cache in vm3");

      vm2.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
      logger.info("Created sender in vm2");

      vm3.invoke(() -> createReceiverInSecuredCache());
      logger.info("Created receiver in vm3");

      vm2.invoke(() -> WANTestBase.startSender( "ln" ));
      vm2.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));

      vm2.invoke(() -> verifyDifferentServerInGetCredentialCall());
      vm3.invoke(() -> verifyDifferentServerInGetCredentialCall());

    }
  }
}
