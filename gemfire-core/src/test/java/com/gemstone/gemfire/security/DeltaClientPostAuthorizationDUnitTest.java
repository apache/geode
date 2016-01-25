
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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.util.Callable;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @since 6.1
 * 
 */
public class DeltaClientPostAuthorizationDUnitTest extends
    ClientAuthorizationTestBase {
  private static final int PAUSE = 5 * 1000;

  /** constructor */
  public DeltaClientPostAuthorizationDUnitTest(String name) {
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

  public void testPutPostOpNotifications() throws Exception {
    addExpectedException("Unexpected IOException");
    addExpectedException("SocketException");

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

        OperationWithAction.OPBLOCK_END };

      AuthzCredentialGenerator gen = this.getXmlAuthzGenerator();
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

  protected void executeOpBlock(List opBlock, Integer port1, Integer port2,
      String authInit, Properties extraAuthProps, Properties extraAuthzProps,
      TestCredentialGenerator gen, Random rnd) {
    Iterator opIter = opBlock.iterator();
    while (opIter.hasNext()) {
      // Start client with valid credentials as specified in
      // OperationWithAction
      OperationWithAction currentOp = (OperationWithAction)opIter.next();
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
      getLogWriter().info(
          "executeOpBlock: performing operation number ["
              + currentOp.getOpNum() + "]: " + currentOp);
      if ((opFlags & OpFlags.USE_OLDCONN) == 0) {
        Properties opCredentials;
        int newRnd = rnd.nextInt(100) + 1;
        String currentRegionName = '/' + regionName;
        if ((opFlags & OpFlags.USE_SUBREGION) > 0) {
          currentRegionName += ('/' + subregionName);
        }
        String credentialsTypeStr;
        OperationCode authOpCode = currentOp.getAuthzOperationCode();
        int[] indices = currentOp.getIndices();
        CredentialGenerator cGen = gen.getCredentialGenerator();
        Properties javaProps = null;
        if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0
            || (opFlags & OpFlags.USE_NOTAUTHZ) > 0) {
          opCredentials = gen.getDisallowedCredentials(
              new OperationCode[] { authOpCode },
              new String[] { currentRegionName }, indices, newRnd);
          credentialsTypeStr = " unauthorized " + authOpCode;
        }
        else {
          opCredentials = gen.getAllowedCredentials(new OperationCode[] {
              opCode, authOpCode }, new String[] { currentRegionName },
              indices, newRnd);
          credentialsTypeStr = " authorized " + authOpCode;
        }
        if (cGen != null) {
          javaProps = cGen.getJavaProperties();
        }
        Properties clientProps = SecurityTestUtil
            .concatProperties(new Properties[] { opCredentials, extraAuthProps,
                extraAuthzProps });
        // Start the client with valid credentials but allowed or disallowed to
        // perform an operation
        getLogWriter().info(
            "executeOpBlock: For client" + clientNum + credentialsTypeStr
                + " credentials: " + opCredentials);
        boolean setupDynamicRegionFactory = (opFlags & OpFlags.ENABLE_DRF) > 0;
        if (useThisVM) {
          createCacheClient(authInit, clientProps, javaProps, new Integer[] {
              port1, port2 }, null, Boolean.valueOf(setupDynamicRegionFactory),
              new Integer(SecurityTestUtil.NO_EXCEPTION));
        }
        else {
          clientVM.invoke(ClientAuthorizationTestBase.class,
              "createCacheClient", new Object[] { authInit, clientProps,
                  javaProps, new Integer[] { port1, port2 }, null,
                  Boolean.valueOf(setupDynamicRegionFactory),
                  new Integer(SecurityTestUtil.NO_EXCEPTION) });
        }
      }
      int expectedResult;
      if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0) {
        expectedResult = SecurityTestUtil.NOTAUTHZ_EXCEPTION;
      }
      else if ((opFlags & OpFlags.CHECK_EXCEPTION) > 0) {
        expectedResult = SecurityTestUtil.OTHER_EXCEPTION;
      }
      else {
        expectedResult = SecurityTestUtil.NO_EXCEPTION;
      }

      // Perform the operation from selected client
      if (useThisVM) {
        doOp(new Byte(opCode.toOrdinal()), currentOp.getIndices(), new Integer(
            opFlags), new Integer(expectedResult));
      }
      else {
        clientVM.invoke(DeltaClientPostAuthorizationDUnitTest.class, "doOp",
            new Object[] { new Byte(opCode.toOrdinal()),
                currentOp.getIndices(), new Integer(opFlags),
                new Integer(expectedResult) });
      }
    }
  }

  private static Region createSubregion(Region region) {

    Region subregion = getSubregion();
    if (subregion == null) {
      subregion = region.createSubregion(subregionName, region.getAttributes());
    }
    return subregion;
  }

  public static void doOp(Byte opCode, int[] indices, Integer flagsI,
      Integer expectedResult) {

    OperationCode op = OperationCode.fromOrdinal(opCode.byteValue());
    boolean operationOmitted = false;
    final int flags = flagsI.intValue();
    Region region = getRegion();
//    for (int i = 0; i < indices.length; i++) {
//      region.put(SecurityTestUtil.keys[i],
//          DeltaClientAuthorizationDUnitTest.deltas[i]);
//    }
    if ((flags & OpFlags.USE_SUBREGION) > 0) {
      assertNotNull(region);
      Region subregion = null;
      if ((flags & OpFlags.NO_CREATE_SUBREGION) > 0) {
        if ((flags & OpFlags.CHECK_NOREGION) > 0) {
          // Wait for some time for DRF update to come
          SecurityTestUtil.waitForCondition(new Callable() {
            public Object call() throws Exception {
              return Boolean.valueOf(getSubregion() == null);
            }
          });
          subregion = getSubregion();
          assertNull(subregion);
          return;
        }
        else {
          // Wait for some time for DRF update to come
          SecurityTestUtil.waitForCondition(new Callable() {
            public Object call() throws Exception {
              return Boolean.valueOf(getSubregion() != null);
            }
          });
          subregion = getSubregion();
          assertNotNull(subregion);
        }
      }
      else {
        subregion = createSubregion(region);
      }
      assertNotNull(subregion);
      region = subregion;
    }
    else if ((flags & OpFlags.CHECK_NOREGION) > 0) {
      // Wait for some time for region destroy update to come
      SecurityTestUtil.waitForCondition(new Callable() {
        public Object call() throws Exception {
          return Boolean.valueOf(getRegion() == null);
        }
      });
      region = getRegion();
      assertNull(region);
      return;
    }
    else {
      assertNotNull(region);
    }
    final String[] keys = SecurityTestUtil.keys;
    final DeltaTestImpl[] vals;
    if ((flags & OpFlags.USE_NEWVAL) > 0) {
      vals = DeltaClientAuthorizationDUnitTest.deltas;
    }
    else {
      vals = DeltaClientAuthorizationDUnitTest.deltas;
    }
    InterestResultPolicy policy = InterestResultPolicy.KEYS_VALUES;
    if ((flags & OpFlags.REGISTER_POLICY_NONE) > 0) {
      policy = InterestResultPolicy.NONE;
    }
    final int numOps = indices.length;
    getLogWriter().info(
        "Got doOp for op: " + op.toString() + ", numOps: " + numOps
            + ", indices: " + indicesToString(indices) + ", expect: " + expectedResult);
    boolean exceptionOccured = false;
    boolean breakLoop = false;
    if (op.isGet()) {
      try {
        Thread.sleep(PAUSE);
      }
      catch (InterruptedException e) {
        fail("interrupted");
      }
    }
    for (int indexIndex = 0; indexIndex < numOps; ++indexIndex) {
      if (breakLoop) {
        break;
      }
      int index = indices[indexIndex];
      try {
        final Object key = keys[index];
        final Object expectedVal = vals[index];
        if (op.isGet()) {
          Object value = null;
          // this is the case for testing GET_ALL
          if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            List keyList = new ArrayList(numOps);
            Object searchKey;
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              searchKey = keys[keyNum];
              keyList.add(searchKey);
              // local invalidate some keys to force fetch of those keys from
              // server
              if ((flags & OpFlags.CHECK_NOKEY) > 0) {
                assertFalse(region.containsKey(searchKey));
              }
              else {
                if (keyNumIndex % 2 == 1) {
                  assertTrue(region.containsKey(searchKey));
                  region.localInvalidate(searchKey);
                }
              }
            }
            Map entries = region.getAll(keyList);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              searchKey = keys[keyNum];
              if ((flags & OpFlags.CHECK_FAIL) > 0) {
                assertFalse(entries.containsKey(searchKey));
              }
              else {
                assertTrue(entries.containsKey(searchKey));
                value = entries.get(searchKey);
                assertEquals(vals[keyNum], value);
              }
            }
            break;
          }
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            Callable cond = new Callable() {
              private Region region;

              public Object call() throws Exception {
                Object value = SecurityTestUtil.getLocalValue(region, key);
                return Boolean
                    .valueOf((flags & OpFlags.CHECK_FAIL) > 0 ? !expectedVal
                        .equals(value) : expectedVal.equals(value));
              }

              public Callable init(Region region) {
                this.region = region;
                return this;
              }
            }.init(region);
            SecurityTestUtil.waitForCondition(cond);
            value = SecurityTestUtil.getLocalValue(region, key);
          }
          else {
            if ((flags & OpFlags.CHECK_NOKEY) > 0) {
              assertFalse(region.containsKey(key));
            }
            else {
              assertTrue(region.containsKey(key));
              region.localInvalidate(key);
            }
            value = region.get(key);
          }
          if ((flags & OpFlags.CHECK_FAIL) > 0) {
            assertFalse(expectedVal.equals(value));
          }
          else {
            assertNotNull(value);
            assertEquals(expectedVal, value);
          }
        }
        else if (op.isPut()) {
          region.put(key, expectedVal);
        }
        else if (op.isRegisterInterest()) {
          if ((flags & OpFlags.USE_LIST) > 0) {
            breakLoop = true;
            // Register interest list in this case
            List keyList = new ArrayList(numOps);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              keyList.add(keys[keyNum]);
            }
            region.registerInterest(keyList, policy);
          }
          else if ((flags & OpFlags.USE_REGEX) > 0) {
            breakLoop = true;
            region.registerInterestRegex("key[1-" + numOps + ']', policy);
          }
          else if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            region.registerInterest("ALL_KEYS", policy);
          }
          else {
            region.registerInterest(key, policy);
          }
        }
        else {
          fail("doOp: Unhandled operation " + op);
        }
        if (expectedResult.intValue() != SecurityTestUtil.NO_EXCEPTION) {
          if (!operationOmitted && !op.isUnregisterInterest()) {
            fail("Expected an exception while performing operation op =" + op +
                "flags = " + OpFlags.description(flags));
          }
        }
      }
      catch (Exception ex) {
        exceptionOccured = true;
        if ((ex instanceof ServerConnectivityException
            || ex instanceof QueryInvocationTargetException || ex instanceof CqException)
            && (expectedResult.intValue() == SecurityTestUtil.NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "doOp: Got expected NotAuthorizedException when doing operation ["
                  + op + "] with flags " + OpFlags.description(flags) 
                  + ": " + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
          getLogWriter().info(
              "doOp: Got expected exception when doing operation: "
                  + ex.toString());
          continue;
        }
        else {
          fail("doOp: Got unexpected exception when doing operation. Policy = " 
              + policy + " flags = " + OpFlags.description(flags), ex);
        }
      }
    }
    if (!exceptionOccured && !operationOmitted
        && expectedResult.intValue() != SecurityTestUtil.NO_EXCEPTION) {
      fail("Expected an exception while performing operation: " + op + 
          " flags = " + OpFlags.description(flags));
    }
  }

}
