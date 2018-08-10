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
package org.apache.geode.security;

import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Tests for authorization from client to server. This tests for authorization with post-process
 * callbacks in case return values of operations and for notifications along-with failover.
 *
 * @since GemFire 5.5
 */
@Category({SecurityTest.class})
public class ClientPostAuthorizationDUnitTest extends ClientAuthorizationTestCase {

  @Test
  public void testAllPostOps() throws Exception {
    OperationWithAction[] allOps = allOpsForTestAllPostOps();

    for (Iterator<AuthzCredentialGenerator> iter = getDummyGeneratorCombos().iterator(); iter
        .hasNext();) {
      AuthzCredentialGenerator gen = iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();
      TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

      getLogWriter().info("testAllPostOps: Using authinit: " + authInit);
      getLogWriter().info("testAllPostOps: Using authenticator: " + authenticator);
      getLogWriter().info("testAllPostOps: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps =
          buildProperties(authenticator, accessor, true, extraAuthProps, extraAuthzProps);

      // Get ports for the servers
      int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      int port1 = randomAvailableTCPPorts[0];
      int port2 = randomAvailableTCPPorts[1];

      // Close down any running servers
      server1.invoke(() -> closeCache());
      server2.invoke(() -> closeCache());

      // Perform all the ops on the clients
      List opBlock = new ArrayList();
      Random rnd = new Random();

      for (int opNum = 0; opNum < allOps.length; ++opNum) {
        // Start client with valid credentials as specified in OperationWithAction
        OperationWithAction currentOp = allOps[opNum];
        if (currentOp.equals(OperationWithAction.OPBLOCK_END)
            || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
          // End of current operation block; execute all the operations on the servers with failover
          if (opBlock.size() > 0) {
            // Start the first server and execute the operation block
            server1
                .invoke(() -> createCacheServer(port1, serverProps, javaProps));
            server2.invoke(() -> closeCache());
            executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
                rnd);
            if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
              // Failover to the second server and run the block again
              server2
                  .invoke(() -> createCacheServer(port2, serverProps, javaProps));
              server1.invoke(() -> closeCache());
              executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
                  rnd);
            }
            opBlock.clear();
          }

        } else {
          currentOp.setOpNum(opNum);
          opBlock.add(currentOp);
        }
      }
    }
  }

  @Test
  public void testAllOpsNotifications() throws Exception {
    OperationWithAction[] allOps = allOpsForTestAllOpsNotifications();

    AuthzCredentialGenerator authzGenerator = getXmlAuthzGenerator();

    getLogWriter().info("Executing opblocks with credential generator " + authzGenerator);

    CredentialGenerator credentialGenerator = authzGenerator.getCredentialGenerator();
    Properties extraAuthProps = credentialGenerator.getSystemProperties();
    Properties javaProps = credentialGenerator.getJavaProperties();
    Properties extraAuthzProps = authzGenerator.getSystemProperties();
    String authenticator = credentialGenerator.getAuthenticator();
    String authInit = credentialGenerator.getAuthInit();
    String accessor = authzGenerator.getAuthorizationCallback();
    TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(authzGenerator);

    getLogWriter().info("testAllOpsNotifications: Using authinit: " + authInit);
    getLogWriter().info("testAllOpsNotifications: Using authenticator: " + authenticator);
    getLogWriter().info("testAllOpsNotifications: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps =
        buildProperties(authenticator, accessor, true, extraAuthProps, extraAuthzProps);

    // Get ports for the servers
    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = randomAvailableTCPPorts[0];
    int port2 = randomAvailableTCPPorts[1];

    // Perform all the ops on the clients
    List opBlock = new ArrayList();
    Random rnd = new Random();

    for (int opNum = 0; opNum < allOps.length; ++opNum) {
      // Start client with valid credentials as specified in OperationWithAction
      OperationWithAction currentOp = allOps[opNum];
      if (currentOp.equals(OperationWithAction.OPBLOCK_END)
          || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
        // End of current operation block; execute all the operations on the servers with failover
        if (opBlock.size() > 0) {
          // Start the first server and execute the operation block
          server1.invoke(() -> createCacheServer(port1, serverProps, javaProps));
          server2.invoke(() -> closeCache());
          executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
              rnd);
          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            // Failover to the second server and run the block again
            server2
                .invoke(() -> createCacheServer(port2, serverProps, javaProps));
            server1.invoke(() -> closeCache());
            executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
                rnd);
          }
          opBlock.clear();
        }

      } else {
        currentOp.setOpNum(opNum);
        opBlock.add(currentOp);
      }
    }
  }

  private OperationWithAction[] allOpsForTestAllPostOps() {
    return new OperationWithAction[] {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY | OpFlags.CHECK_NOTAUTHZ,
            4),

        // OPBLOCK_END indicates end of an operation block that needs to be executed on each server
        // when doing failover
        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.CHECK_NOKEY | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOTAUTHZ,
            4),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a KEY_SET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 6),
        new OperationWithAction(OperationCode.KEY_SET, 2, OpFlags.NONE, 6),
        new OperationWithAction(OperationCode.KEY_SET, 3, OpFlags.CHECK_NOTAUTHZ, 6),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a QUERY
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 7),
        new OperationWithAction(OperationCode.QUERY, 2, OpFlags.USE_NEWVAL, 7),
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.USE_NEWVAL | OpFlags.CHECK_NOTAUTHZ,
            7),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a EXECUTE_CQ initial results
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 2, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3, OpFlags.CHECK_NOTAUTHZ, 8),

        OperationWithAction.OPBLOCK_END};
  }

  private OperationWithAction[] allOpsForTestAllOpsNotifications() {
    return new OperationWithAction[] {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 2,
            OpFlags.USE_REGEX | OpFlags.REGISTER_POLICY_NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 3,
            OpFlags.USE_REGEX | OpFlags.REGISTER_POLICY_NONE | OpFlags.USE_NOTAUTHZ, 8),
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.CHECK_FAIL, 4),

        // OPBLOCK_END indicates end of an operation block that needs to be executed on each server
        // when doing failover
        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 2,
            OpFlags.USE_REGEX | OpFlags.REGISTER_POLICY_NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 3,
            OpFlags.USE_REGEX | OpFlags.REGISTER_POLICY_NONE | OpFlags.USE_NOTAUTHZ, 8),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.USE_NEWVAL | OpFlags.CHECK_FAIL, 4),

        OperationWithAction.OPBLOCK_END,

        // Test DESTROY and verify with GET that KEYS should not exist
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 2,
            OpFlags.USE_REGEX, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.USE_REGEX | OpFlags.USE_OLDCONN | OpFlags.REGISTER_POLICY_NONE, 8),
        // registerInterest now clears the KEYS, so a dummy put to add those KEYS back for the case
        // when updates should not come
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.DESTROY, 1, OpFlags.USE_OLDCONN, 4),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.CHECK_FAIL, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_CLEAR and check with GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 2,
            OpFlags.USE_ALL_KEYS, 1),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.USE_ALL_KEYS | OpFlags.USE_OLDCONN | OpFlags.REGISTER_POLICY_NONE, 1),
        // registerInterest now clears the KEYS, so a dummy put to add those KEYS back for the case
        // when updates should not come
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.REGION_CLEAR, 1, OpFlags.USE_OLDCONN, 1),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.CHECK_FAIL, 8),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 8),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_CREATE and check with CREATE/GET
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 2,
            OpFlags.USE_ALL_KEYS | OpFlags.ENABLE_DRF, 1),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 3,
            OpFlags.USE_ALL_KEYS | OpFlags.ENABLE_DRF | OpFlags.USE_NOTAUTHZ
                | OpFlags.REGISTER_POLICY_NONE,
            1),
        new OperationWithAction(OperationCode.REGION_CREATE, 1, OpFlags.ENABLE_DRF, 1),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION,
            4),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
                | OpFlags.NO_CREATE_SUBREGION,
            4),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
                | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOREGION,
            4),

        // Do REGION_DESTROY of the sub-region and check with GET
        new OperationWithAction(OperationCode.REGION_DESTROY, 1,
            OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION, 1),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
                | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOREGION,
            4),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_EXCEPTION,
            4),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.USE_SUBREGION
                | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOREGION,
            4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION
            | OpFlags.CHECK_NOKEY | OpFlags.CHECK_EXCEPTION, 4),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_DESTROY of the region and check with GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.NONE, 8),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.GET, 2,
            OpFlags.USE_ALL_KEYS, 1),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.USE_ALL_KEYS | OpFlags.USE_OLDCONN | OpFlags.REGISTER_POLICY_NONE, 1),
        // registerInterest now clears the KEYS, so a dummy put to add those KEYS back for the case
        // when updates should not come
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN, 8),
        new OperationWithAction(OperationCode.REGION_DESTROY, 1, OpFlags.NONE, 1),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP | OpFlags.CHECK_NOREGION, 4),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_NO_FAILOVER};
  }
}
