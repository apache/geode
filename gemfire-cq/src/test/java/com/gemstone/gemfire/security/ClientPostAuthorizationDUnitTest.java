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
package com.gemstone.gemfire.security;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Host;

/**
 * Tests for authorization from client to server. This tests for authorization
 * with post-process callbacks in case return values of operations and for
 * notifications along-with failover.
 * 
 * @author sumedh
 * @since 5.5
 */
public class ClientPostAuthorizationDUnitTest extends
    ClientAuthorizationTestBase {


  /** constructor */
  public ClientPostAuthorizationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {

    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);

    server1.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { serverExpectedExceptions });
    server2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { serverExpectedExceptions });
    client2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { clientExpectedExceptions });
    SecurityTestUtil.registerExpectedExceptions(clientExpectedExceptions);
  }

  // Region: Tests

  public void testAllPostOps() {

    OperationWithAction[] allOps = {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY
            | OpFlags.CHECK_NOTAUTHZ, 4),

        // OPBLOCK_END indicates end of an operation block that needs to
        // be executed on each server when doing failover
        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.CHECK_NOKEY | OpFlags.USE_NEWVAL
                | OpFlags.CHECK_NOTAUTHZ, 4),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a KEY_SET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 6),
        new OperationWithAction(OperationCode.KEY_SET, 2, OpFlags.NONE, 6),
        new OperationWithAction(OperationCode.KEY_SET, 3,
            OpFlags.CHECK_NOTAUTHZ, 6),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a QUERY
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 7),
        new OperationWithAction(OperationCode.QUERY, 2, OpFlags.USE_NEWVAL, 7),
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.USE_NEWVAL
            | OpFlags.CHECK_NOTAUTHZ, 7),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a EXECUTE_CQ initial results
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 2, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3,
            OpFlags.CHECK_NOTAUTHZ, 8),

        OperationWithAction.OPBLOCK_END };

    Iterator iter = getDummyGeneratorCombos().iterator();
    while (iter.hasNext()) {
      AuthzCredentialGenerator gen = (AuthzCredentialGenerator)iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();
      TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

      getLogWriter().info("testAllPostOps: Using authinit: " + authInit);
      getLogWriter().info(
          "testAllPostOps: Using authenticator: " + authenticator);
      getLogWriter().info("testAllPostOps: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, true,
          extraAuthProps, extraAuthzProps);
      // Get ports for the servers
      Integer port1 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
      Integer port2 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));

      // Close down any running servers
      server1.invoke(SecurityTestUtil.class, "closeCache");
      server2.invoke(SecurityTestUtil.class, "closeCache");

      // Perform all the ops on the clients
      List opBlock = new ArrayList();
      Random rnd = new Random();
      for (int opNum = 0; opNum < allOps.length; ++opNum) {
        // Start client with valid credentials as specified in
        // OperationWithAction
        OperationWithAction currentOp = allOps[opNum];
        if (currentOp.equals(OperationWithAction.OPBLOCK_END)
            || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
          // End of current operation block; execute all the operations
          // on the servers with failover
          if (opBlock.size() > 0) {
            // Start the first server and execute the operation block
            server1.invoke(ClientAuthorizationTestBase.class,
                "createCacheServer", new Object[] {
                    SecurityTestUtil.getLocatorPort(), port1, serverProps,
                    javaProps });
            server2.invoke(SecurityTestUtil.class, "closeCache");
            executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                extraAuthzProps, tgen, rnd);
            if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
              // Failover to the second server and run the block again
              server2.invoke(ClientAuthorizationTestBase.class,
                  "createCacheServer", new Object[] {
                      SecurityTestUtil.getLocatorPort(), port2, serverProps,
                      javaProps });
              server1.invoke(SecurityTestUtil.class, "closeCache");
              executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                  extraAuthzProps, tgen, rnd);
            }
            opBlock.clear();
          }
        }
        else {
          currentOp.setOpNum(opNum);
          opBlock.add(currentOp);
        }
      }
    }
  }

  public void testAllOpsNotifications() {

    OperationWithAction[] allOps = {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 2, OpFlags.USE_REGEX
                | OpFlags.REGISTER_POLICY_NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 3, OpFlags.USE_REGEX
                | OpFlags.REGISTER_POLICY_NONE | OpFlags.USE_NOTAUTHZ, 8),
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.CHECK_FAIL, 4),

        // OPBLOCK_END indicates end of an operation block that needs to
        // be executed on each server when doing failover
        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 2, OpFlags.USE_REGEX
                | OpFlags.REGISTER_POLICY_NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 3, OpFlags.USE_REGEX
                | OpFlags.REGISTER_POLICY_NONE | OpFlags.USE_NOTAUTHZ, 8),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.USE_NEWVAL | OpFlags.CHECK_FAIL, 4),

        OperationWithAction.OPBLOCK_END,

        // Test DESTROY and verify with GET that keys should not exist
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 2, OpFlags.USE_REGEX, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.USE_REGEX | OpFlags.USE_OLDCONN | OpFlags.REGISTER_POLICY_NONE, 8),
        // registerInterest now clears the keys, so a dummy put to add
        // those keys back for the case when updates should not come
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.DESTROY, 1, OpFlags.USE_OLDCONN,
            4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.CHECK_FAIL, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_CLEAR and check with GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 2, OpFlags.USE_ALL_KEYS, 1),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.USE_ALL_KEYS | OpFlags.USE_OLDCONN | OpFlags.REGISTER_POLICY_NONE, 1),
        // registerInterest now clears the keys, so a dummy put to add
        // those keys back for the case when updates should not come
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.REGION_CLEAR, 1,
            OpFlags.USE_OLDCONN, 1),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.CHECK_FAIL, 8),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 8),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_CREATE and check with CREATE/GET
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 2, OpFlags.USE_ALL_KEYS | OpFlags.ENABLE_DRF, 1),
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 3, OpFlags.USE_ALL_KEYS | OpFlags.ENABLE_DRF
                | OpFlags.USE_NOTAUTHZ | OpFlags.REGISTER_POLICY_NONE, 1),
        new OperationWithAction(OperationCode.REGION_CREATE, 1,
            OpFlags.ENABLE_DRF, 1),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_SUBREGION, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
            | OpFlags.NO_CREATE_SUBREGION, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
            | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOREGION, 4),

        // Do REGION_DESTROY of the sub-region and check with GET
        new OperationWithAction(OperationCode.REGION_DESTROY, 1,
            OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION, 1),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
            | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOREGION, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_SUBREGION | OpFlags.CHECK_NOKEY
            | OpFlags.CHECK_EXCEPTION, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
            | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOREGION, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.USE_SUBREGION | OpFlags.CHECK_NOKEY
            | OpFlags.CHECK_EXCEPTION, 4),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_DESTROY of the region and check with GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.GET, 2, OpFlags.USE_ALL_KEYS, 1),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.USE_ALL_KEYS | OpFlags.USE_OLDCONN | OpFlags.REGISTER_POLICY_NONE, 1),
        // registerInterest now clears the keys, so a dummy put to add
        // those keys back for the case when updates should not come
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.REGION_DESTROY, 1, OpFlags.NONE,
            1),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP | OpFlags.CHECK_NOREGION, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_NO_FAILOVER };

      AuthzCredentialGenerator gen = getXmlAuthzGenerator();
      getLogWriter().info("Executing opblocks with credential generator " + gen);
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();
      TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

      getLogWriter().info(
          "testAllOpsNotifications: Using authinit: " + authInit);
      getLogWriter().info(
          "testAllOpsNotifications: Using authenticator: " + authenticator);
      getLogWriter().info(
          "testAllOpsNotifications: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, true,
          extraAuthProps, extraAuthzProps);
      // Get ports for the servers
      Integer port1 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
      Integer port2 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));

      // Perform all the ops on the clients
      List opBlock = new ArrayList();
      Random rnd = new Random();
      for (int opNum = 0; opNum < allOps.length; ++opNum) {
        // Start client with valid credentials as specified in
        // OperationWithAction
        OperationWithAction currentOp = allOps[opNum];
        if (currentOp.equals(OperationWithAction.OPBLOCK_END)
            || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
          // End of current operation block; execute all the operations
          // on the servers with failover
          if (opBlock.size() > 0) {
            // Start the first server and execute the operation block
            server1.invoke(ClientAuthorizationTestBase.class,
                "createCacheServer", new Object[] {
                    SecurityTestUtil.getLocatorPort(), port1, serverProps,
                    javaProps });
            server2.invoke(SecurityTestUtil.class, "closeCache");
            executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                extraAuthzProps, tgen, rnd);
            if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
              // Failover to the second server and run the block again
              server2.invoke(ClientAuthorizationTestBase.class,
                  "createCacheServer", new Object[] {
                      SecurityTestUtil.getLocatorPort(), port2, serverProps,
                      javaProps });
              server1.invoke(SecurityTestUtil.class, "closeCache");
              executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                  extraAuthzProps, tgen, rnd);
            }
            opBlock.clear();
          }
        }
        else {
          currentOp.setOpNum(opNum);
          opBlock.add(currentOp);
        }
      }
  }

  // End Region: Tests

  public void tearDown2() throws Exception {

    super.tearDown2();
    // close the clients first
    client1.invoke(SecurityTestUtil.class, "closeCache");
    client2.invoke(SecurityTestUtil.class, "closeCache");
    SecurityTestUtil.closeCache();
    // then close the servers
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");
  }

}
