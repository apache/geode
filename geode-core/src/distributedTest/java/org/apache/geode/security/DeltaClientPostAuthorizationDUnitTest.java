/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static org.apache.geode.security.SecurityTestUtils.NOTAUTHZ_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.NO_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.OTHER_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.security.SecurityTestUtils.concatProperties;
import static org.apache.geode.security.SecurityTestUtils.createCacheClient;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DeltaTestImpl;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.TestObjectWithIdentifier;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * @since GemFire 6.1
 */
@Category({SecurityTest.class})
public class DeltaClientPostAuthorizationDUnitTest extends ClientAuthorizationTestCase {

  private static final int PAUSE = 5 * 1000; // TODO: replace with Awaitility

  private DeltaTestImpl[] deltas = new DeltaTestImpl[8];

  @Override
  public final void preSetUpClientAuthorizationTestBase() throws Exception {
    setUpDeltas();
    addIgnoredException("Unexpected IOException");
    addIgnoredException("SocketException");
  }

  @Override
  public final void preTearDownClientAuthorizationTestBase() throws Exception {
    closeCache();
  }

  @Test
  public void testPutPostOpNotifications() throws Exception {
    OperationWithAction[] allOps = allOps();

    AuthzCredentialGenerator gen = this.getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();
    TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

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
          server1.invoke(
              () -> ClientAuthorizationTestCase.createCacheServer(port1, serverProps, javaProps));
          server2.invoke(() -> closeCache());

          executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
              rnd);

          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            // Failover to the second server and run the block again
            server2.invoke(
                () -> ClientAuthorizationTestCase.createCacheServer(port2, serverProps, javaProps));
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

  @Override
  protected void executeOpBlock(final List<OperationWithAction> opBlock, final int port1,
      final int port2, final String authInit, final Properties extraAuthProps,
      final Properties extraAuthzProps, final TestCredentialGenerator credentialGenerator,
      final Random random) throws InterruptedException {
    for (Iterator<OperationWithAction> opIter = opBlock.iterator(); opIter.hasNext();) {
      // Start client with valid credentials as specified in OperationWithAction
      OperationWithAction currentOp = opIter.next();
      OperationCode opCode = currentOp.getOperationCode();
      int opFlags = currentOp.getFlags();
      int clientNum = currentOp.getClientNum();
      VM clientVM = null;
      boolean useThisVM = false;

      switch (clientNum) {
        case 1:
          clientVM = client1;
          break;
        case 2:
          clientVM = client2;
          break;
        case 3:
          useThisVM = true;
          break;
        default:
          fail("executeOpBlock: Unknown client number " + clientNum);
          break;
      }

      getLogWriter().info("executeOpBlock: performing operation number [" + currentOp.getOpNum()
          + "]: " + currentOp);

      if ((opFlags & OpFlags.USE_OLDCONN) == 0) {
        Properties opCredentials;
        int newRnd = random.nextInt(100) + 1;
        String currentRegionName = '/' + regionName;
        if ((opFlags & OpFlags.USE_SUBREGION) > 0) {
          currentRegionName += ('/' + SUBREGION_NAME);
        }

        String credentialsTypeStr;
        OperationCode authOpCode = currentOp.getAuthzOperationCode();
        int[] indices = currentOp.getIndices();
        CredentialGenerator cGen = credentialGenerator.getCredentialGenerator();
        final Properties javaProps = cGen == null ? null : cGen.getJavaProperties();

        if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0 || (opFlags & OpFlags.USE_NOTAUTHZ) > 0) {
          opCredentials = credentialGenerator.getDisallowedCredentials(
              new OperationCode[] {authOpCode}, new String[] {currentRegionName}, indices, newRnd);
          credentialsTypeStr = " unauthorized " + authOpCode;

        } else {
          opCredentials =
              credentialGenerator.getAllowedCredentials(new OperationCode[] {opCode, authOpCode},
                  new String[] {currentRegionName}, indices, newRnd);
          credentialsTypeStr = " authorized " + authOpCode;
        }

        Properties clientProps =
            concatProperties(new Properties[] {opCredentials, extraAuthProps, extraAuthzProps});

        // Start the client with valid credentials but allowed or disallowed to perform an operation
        getLogWriter().info("executeOpBlock: For client" + clientNum + credentialsTypeStr
            + " credentials: " + opCredentials);
        boolean setupDynamicRegionFactory = (opFlags & OpFlags.ENABLE_DRF) > 0;
        if (useThisVM) {
          createCacheClient(authInit, clientProps, javaProps, new int[] {port1, port2}, 0,
              setupDynamicRegionFactory, NO_EXCEPTION);

        } else {
          clientVM.invoke(() -> createCacheClient(authInit, clientProps, javaProps,
              new int[] {port1, port2}, 0, setupDynamicRegionFactory, NO_EXCEPTION));
        }
      }

      int expectedResult;
      if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0) {
        expectedResult = NOTAUTHZ_EXCEPTION;
      } else if ((opFlags & OpFlags.CHECK_EXCEPTION) > 0) {
        expectedResult = OTHER_EXCEPTION;
      } else {
        expectedResult = NO_EXCEPTION;
      }

      // Perform the operation from selected client
      if (useThisVM) {
        doOp(opCode, currentOp.getIndices(), new Integer(opFlags), new Integer(expectedResult));
      } else {
        int[] indices = currentOp.getIndices();
        clientVM.invoke(() -> DeltaClientPostAuthorizationDUnitTest.doOp(opCode, indices,
            new Integer(opFlags), new Integer(expectedResult)));
      }
    }
  }

  private void setUpDeltas() {
    for (int i = 0; i < 8; i++) {
      deltas[i] = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObjectWithIdentifier("0", 0));
    }
    deltas[1].setIntVar(5);
    deltas[2].setIntVar(5);
    deltas[3].setIntVar(5);
    deltas[4].setIntVar(5);
    deltas[5].setIntVar(5);
    deltas[6].setIntVar(5);
    deltas[7].setIntVar(5);

    deltas[2].resetDeltaStatus();
    deltas[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltas[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltas[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltas[5].setByteArr(new byte[] {1, 2, 3, 4, 5});
    // deltas[6].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    // deltas[7].setByteArr(new byte[] { 1, 2, 3, 4, 5 });

    deltas[3].resetDeltaStatus();
    deltas[3].setDoubleVar(new Double(5));
    deltas[4].setDoubleVar(new Double(5));
    deltas[5].setDoubleVar(new Double(5));
    deltas[6].setDoubleVar(new Double(5));
    deltas[7].setDoubleVar(new Double(5));

    deltas[4].resetDeltaStatus();
    deltas[4].setStr("str changed");
    deltas[5].setStr("str changed");
    deltas[6].setStr("str changed");
    // deltas[7].setStr("str changed");

    deltas[5].resetDeltaStatus();
    deltas[5].setIntVar(100);
    deltas[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
    deltas[6].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
    deltas[7].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));

    deltas[6].resetDeltaStatus();
    deltas[6].setByteArr(new byte[] {1, 2, 3});
    deltas[7].setByteArr(new byte[] {1, 2, 3});

    deltas[7].resetDeltaStatus();
    deltas[7].setStr("delta string");
  }

  private OperationWithAction[] allOps() {
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

        OperationWithAction.OPBLOCK_END};
  }
}
