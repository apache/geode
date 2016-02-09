
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
import java.util.Properties;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;
import security.DummyCredentialGenerator;
import security.XmlAuthzCredentialGenerator;

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;

import templates.security.UserPasswordAuthInit;

/**
 * Tests for authorization from client to server. This tests for authorization
 * of all operations with both valid and invalid credentials/modules with
 * pre-operation callbacks. It also checks for authorization in case of
 * failover.
 * 
 * @author sumedh
 * @since 5.5
 */
public class ClientAuthorizationDUnitTest extends ClientAuthorizationTestBase {

  /** constructor */
  public ClientAuthorizationDUnitTest(String name) {
    super(name);
  }

  @Override
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
    client1.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { clientExpectedExceptions });
    client2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] { clientExpectedExceptions });
    SecurityTestUtil.registerExpectedExceptions(clientExpectedExceptions);
  }

  private Properties getUserPassword(String userName) {

    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
    return props;
  }

  private void executeRIOpBlock(List opBlock, Integer port1, Integer port2,
      String authInit, Properties extraAuthProps, Properties extraAuthzProps,
      Properties javaProps) {

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
          fail("executeRIOpBlock: Unknown client number " + clientNum);
          break;
      }
      LogWriterUtils.getLogWriter().info(
          "executeRIOpBlock: performing operation number ["
              + currentOp.getOpNum() + "]: " + currentOp);
      if ((opFlags & OpFlags.USE_OLDCONN) == 0) {
        Properties opCredentials = null;
        String currentRegionName = '/' + regionName;
        if ((opFlags & OpFlags.USE_SUBREGION) > 0) {
          currentRegionName += ('/' + subregionName);
        }
        String credentialsTypeStr;
        OperationCode authOpCode = currentOp.getAuthzOperationCode();
        if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0
            || (opFlags & OpFlags.USE_NOTAUTHZ) > 0
            || !authOpCode.equals(opCode)) {
          credentialsTypeStr = " unauthorized " + authOpCode;
          if (authOpCode.isRegisterInterest()) {
            opCredentials = getUserPassword("reader7");
          }
          else if (authOpCode.isUnregisterInterest()) {
            opCredentials = getUserPassword("reader6");
          }
          else {
            fail("executeRIOpBlock: cannot determine credentials for"
                + credentialsTypeStr);
          }
        }
        else {
          credentialsTypeStr = " authorized " + authOpCode;
          if (authOpCode.isRegisterInterest()
              || authOpCode.isUnregisterInterest()) {
            opCredentials = getUserPassword("reader5");
          }
          else if (authOpCode.isPut()) {
            opCredentials = getUserPassword("writer1");
          }
          else if (authOpCode.isGet()) {
            opCredentials = getUserPassword("reader1");
          }
          else {
            fail("executeRIOpBlock: cannot determine credentials for"
                + credentialsTypeStr);
          }
        }
        Properties clientProps = SecurityTestUtil
            .concatProperties(new Properties[] { opCredentials, extraAuthProps,
                extraAuthzProps });
        // Start the client with valid credentials but allowed or disallowed to
        // perform an operation
        LogWriterUtils.getLogWriter().info(
            "executeRIOpBlock: For client" + clientNum + credentialsTypeStr
                + " credentials: " + opCredentials);
        if (useThisVM) {
          createCacheClient(authInit, clientProps, javaProps, new Integer[] {
              port1, port2 }, null, Boolean.valueOf(false), new Integer(
              SecurityTestUtil.NO_EXCEPTION));
        }
        else {
          clientVM.invoke(ClientAuthorizationTestBase.class,
              "createCacheClient", new Object[] { authInit, clientProps,
                  javaProps, new Integer[] { port1, port2 }, null,
                  Boolean.valueOf(false),
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
        clientVM.invoke(ClientAuthorizationTestBase.class, "doOp",
            new Object[] { new Byte(opCode.toOrdinal()),
                currentOp.getIndices(), new Integer(opFlags),
                new Integer(expectedResult) });
      }
    }
  }

  // Region: Tests

  public void testAllowPutsGets() {
      AuthzCredentialGenerator gen = getXmlAuthzGenerator();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      LogWriterUtils.getLogWriter().info("testAllowPutsGets: Using authinit: " + authInit);
      LogWriterUtils.getLogWriter().info(
          "testAllowPutsGets: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter().info("testAllowPutsGets: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthorizationTestBase.class, "createCacheServer", new Object[] {
              SecurityTestUtil.getLocatorPort(), serverProps, javaProps }));
      Integer port2 = ((Integer)server2.invoke(
          ClientAuthorizationTestBase.class, "createCacheServer", new Object[] {
              SecurityTestUtil.getLocatorPort(), serverProps, javaProps }));

      // Start client1 with valid CREATE credentials
      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      javaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testAllowPutsGets: For first client credentials: "
              + createCredentials);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, createCredentials, javaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Start client2 with valid GET credentials
      Properties getCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 2);
      javaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter()
          .info(
              "testAllowPutsGets: For second client credentials: "
                  + getCredentials);
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, javaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Verify that the gets succeed
      client2.invoke(SecurityTestUtil.class, "doGets", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION) });
  }

  public void testDisallowPutsGets() {

      AuthzCredentialGenerator gen = getXmlAuthzGenerator();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      LogWriterUtils.getLogWriter().info("testDisallowPutsGets: Using authinit: " + authInit);
      LogWriterUtils.getLogWriter().info(
          "testDisallowPutsGets: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter().info("testDisallowPutsGets: Using accessor: " + accessor);

      // Check that we indeed can obtain valid credentials not allowed to do
      // gets
      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties createJavaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testDisallowPutsGets: For first client credentials: "
              + createCredentials);
      Properties getCredentials = gen.getDisallowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 2);
      Properties getJavaProps = cGen.getJavaProperties();

      LogWriterUtils.getLogWriter().info(
          "testDisallowPutsGets: For second client disallowed GET credentials: "
              + getCredentials);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthorizationTestBase.class, "createCacheServer", new Object[] {
              SecurityTestUtil.getLocatorPort(), serverProps, javaProps }));
      Integer port2 = ((Integer)server2.invoke(
          ClientAuthorizationTestBase.class, "createCacheServer", new Object[] {
              SecurityTestUtil.getLocatorPort(), serverProps, javaProps }));

      // Start client1 with valid CREATE credentials
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, createCredentials, createJavaProps, port1,
              port2, null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Start client2 with invalid GET credentials
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, getJavaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Gets as normal user should throw exception
      client2.invoke(SecurityTestUtil.class, "doGets", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NOTAUTHZ_EXCEPTION) });

      // Try to connect client2 with reader credentials
      getCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 5);
      getJavaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testDisallowPutsGets: For second client with GET credentials: "
              + getCredentials);
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, getJavaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Verify that the gets succeed
      client2.invoke(SecurityTestUtil.class, "doGets", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Verify that the puts throw exception
      client2.invoke(SecurityTestUtil.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NOTAUTHZ_EXCEPTION) });
  }

  public void testInvalidAccessor() {
      AuthzCredentialGenerator gen = getXmlAuthzGenerator();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      LogWriterUtils.getLogWriter().info("testInvalidAccessor: Using authinit: " + authInit);
      LogWriterUtils.getLogWriter().info(
          "testInvalidAccessor: Using authenticator: " + authenticator);

      // Start server1 with invalid accessor
      Properties serverProps = buildProperties(authenticator,
          "com.gemstone.none", false, extraAuthProps, extraAuthzProps);
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthorizationTestBase.class, "createCacheServer", new Object[] {
              SecurityTestUtil.getLocatorPort(), serverProps, javaProps }));
      Integer port2 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));

      // Client creation should throw exceptions
      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 3);
      Properties createJavaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidAccessor: For first client CREATE credentials: "
              + createCredentials);
      Properties getCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 7);
      Properties getJavaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testInvalidAccessor: For second client GET credentials: "
              + getCredentials);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, createCredentials, createJavaProps, port1,
              port2, null, Boolean.FALSE, Boolean.FALSE,
              Integer.valueOf(SecurityTestUtil.NO_EXCEPTION) });
      client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(1), new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, getJavaProps, port1, port2,
              null, Boolean.FALSE, Boolean.FALSE,
              Integer.valueOf(SecurityTestUtil.NO_EXCEPTION) });
      client2.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(1), new Integer(SecurityTestUtil.AUTHFAIL_EXCEPTION) });

      // Now start server2 that has valid accessor
      LogWriterUtils.getLogWriter().info("testInvalidAccessor: Using accessor: " + accessor);
      serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      server2.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] { SecurityTestUtil.getLocatorPort(), port2, serverProps,
              javaProps });
      server1.invoke(SecurityTestUtil.class, "closeCache");

      // Client creation should be successful now
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, createCredentials, createJavaProps, port1,
              port2, null, new Integer(SecurityTestUtil.NO_EXCEPTION) });
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, getJavaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Now perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Verify that the gets succeed
      client2.invoke(SecurityTestUtil.class, "doGets", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });
  }

  public void testPutsGetsWithFailover() {
      AuthzCredentialGenerator gen = getXmlAuthzGenerator();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      LogWriterUtils.getLogWriter().info(
          "testPutsGetsWithFailover: Using authinit: " + authInit);
      LogWriterUtils.getLogWriter().info(
          "testPutsGetsWithFailover: Using authenticator: " + authenticator);
      LogWriterUtils.getLogWriter().info(
          "testPutsGetsWithFailover: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Integer port1 = ((Integer)server1.invoke(
          ClientAuthorizationTestBase.class, "createCacheServer", new Object[] {
              SecurityTestUtil.getLocatorPort(), serverProps, javaProps }));
      // Get a port for second server but do not start it
      // This forces the clients to connect to the first server
      Integer port2 = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));

      // Start client1 with valid CREATE credentials
      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties createJavaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testPutsGetsWithFailover: For first client credentials: "
              + createCredentials);
      client1.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, createCredentials, createJavaProps, port1,
              port2, null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Start client2 with valid GET credentials
      Properties getCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 5);
      Properties getJavaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testPutsGetsWithFailover: For second client credentials: "
              + getCredentials);
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, getJavaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION) });
      // Verify that the puts succeeded
      client2.invoke(SecurityTestUtil.class, "doGets", new Object[] {
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // start the second one and stop the first server to force a failover
      server2.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] { SecurityTestUtil.getLocatorPort(), port2, serverProps,
              javaProps });
      server1.invoke(SecurityTestUtil.class, "closeCache");

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doNPuts", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });
      // Verify that the puts succeeded
      client2.invoke(SecurityTestUtil.class, "doNGets", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Now re-connect with credentials not allowed to do gets
      Properties noGetCredentials = gen.getDisallowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 9);
      getJavaProps = cGen.getJavaProperties();

      LogWriterUtils.getLogWriter().info(
          "testPutsGetsWithFailover: For second client disallowed GET credentials: "
              + noGetCredentials);

      // Re-connect client2 with invalid GET credentials
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, noGetCredentials, getJavaProps, port1,
              port2, null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });
      // Gets as normal user should throw exception
      client2.invoke(SecurityTestUtil.class, "doGets", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NOTAUTHZ_EXCEPTION) });

      // force a failover and do the drill again
      server1.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] { SecurityTestUtil.getLocatorPort(), port1, serverProps,
              javaProps });
      server2.invoke(SecurityTestUtil.class, "closeCache");

      // Perform some put operations from client1
      client1.invoke(SecurityTestUtil.class, "doNPuts", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });
      // Gets as normal user should throw exception
      client2.invoke(SecurityTestUtil.class, "doNGets", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NOTAUTHZ_EXCEPTION) });

      // Try to connect client2 with reader credentials
      client2.invoke(ClientAuthenticationDUnitTest.class, "createCacheClient",
          new Object[] { authInit, getCredentials, getJavaProps, port1, port2,
              null, new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Verify that the gets succeed
      client2.invoke(SecurityTestUtil.class, "doNGets", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NO_EXCEPTION) });

      // Verify that the puts throw exception
      client2.invoke(SecurityTestUtil.class, "doPuts", new Object[] {
          new Integer(4), new Integer(SecurityTestUtil.NOTAUTHZ_EXCEPTION) });
  }

  public void testUnregisterInterestWithFailover() {

    OperationWithAction[] unregisterOps = {
        // Register interest in all keys using one key at a time
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.UNREGISTER_INTEREST, 3, OpFlags.NONE, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all keys using one key at a time
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 3,
            OpFlags.USE_OLDCONN | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2,
            OpFlags.USE_OLDCONN, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all keys using list
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_LIST, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 1,
            OpFlags.USE_LIST, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT, 2),
        new OperationWithAction(OperationCode.GET, 1, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all keys using list
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_LIST | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 1,
            OpFlags.USE_OLDCONN | OpFlags.USE_LIST, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 1, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all keys using regular expression
        new OperationWithAction(OperationCode.REGISTER_INTEREST,
            OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_REGEX, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2,
            OpFlags.USE_REGEX, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all keys using regular expression
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_REGEX | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_REGEX, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END };

    AuthzCredentialGenerator gen = new XmlAuthzCredentialGenerator();
    CredentialGenerator cGen = new DummyCredentialGenerator();
    cGen.init();
    gen.init(cGen);
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    LogWriterUtils.getLogWriter().info("testAllOpsWithFailover: Using authinit: " + authInit);
    LogWriterUtils.getLogWriter().info(
        "testAllOpsWithFailover: Using authenticator: " + authenticator);
    LogWriterUtils.getLogWriter().info("testAllOpsWithFailover: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, accessor, false,
        extraAuthProps, extraAuthzProps);
    // Get ports for the servers
    Integer port1 = new Integer(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));
    Integer port2 = new Integer(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));

    // Perform all the ops on the clients
    List opBlock = new ArrayList();
    for (int opNum = 0; opNum < unregisterOps.length; ++opNum) {
      // Start client with valid credentials as specified in
      // OperationWithAction
      OperationWithAction currentOp = unregisterOps[opNum];
      if (currentOp.equals(OperationWithAction.OPBLOCK_END)
          || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
        // End of current operation block; execute all the operations
        // on the servers with/without failover
        if (opBlock.size() > 0) {
          // Start the first server and execute the operation block
          server1.invoke(ClientAuthorizationTestBase.class,
              "createCacheServer", new Object[] {
                  SecurityTestUtil.getLocatorPort(), port1, serverProps,
                  javaProps });
          server2.invoke(SecurityTestUtil.class, "closeCache");
          executeRIOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
              extraAuthzProps, javaProps);
          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            // Failover to the second server and run the block again
            server2.invoke(ClientAuthorizationTestBase.class,
                "createCacheServer", new Object[] {
                    SecurityTestUtil.getLocatorPort(), port2, serverProps,
                    javaProps });
            server1.invoke(SecurityTestUtil.class, "closeCache");
            executeRIOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                extraAuthzProps, javaProps);
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

  
  public void testAllOpsWithFailover() {
    IgnoredException.addIgnoredException("Read timed out");

    OperationWithAction[] allOps = {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY
            | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY, 4),

        // OPBLOCK_END indicates end of an operation block; the above block of
        // three operations will be first executed on server1 and then on
        // server2 after failover
        OperationWithAction.OPBLOCK_END,

        // Test PUTALL and verify with GETs
        new OperationWithAction(OperationCode.PUTALL, 3, OpFlags.USE_NEWVAL
            | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUTALL, 1, OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        OperationWithAction.OPBLOCK_END,
        
        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_NEWVAL
            | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),

        OperationWithAction.OPBLOCK_END,

        // Test DESTROY and verify with a GET and that key should not exist
        new OperationWithAction(OperationCode.DESTROY, 3, OpFlags.USE_NEWVAL
            | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.DESTROY),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.CHECK_FAIL, 4), // bruce: added check_nokey because we now bring tombstones to the client in 8.0
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_NEWVAL, 4),

        OperationWithAction.OPBLOCK_END,

        // Check CONTAINS_KEY
        new OperationWithAction(OperationCode.CONTAINS_KEY, 3,
            OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.CONTAINS_KEY),
        // Destroy the keys and check for failure in CONTAINS_KEY
        new OperationWithAction(OperationCode.DESTROY, 2),
        new OperationWithAction(OperationCode.CONTAINS_KEY, 3,
            OpFlags.CHECK_FAIL | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.CONTAINS_KEY, 1,
            OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL, 4),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT),

        OperationWithAction.OPBLOCK_END,

        // Check KEY_SET
        new OperationWithAction(OperationCode.KEY_SET, 3,
            OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.KEY_SET, 2),

        OperationWithAction.OPBLOCK_END,

        // Check QUERY
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.CHECK_NOTAUTHZ,
            4),
        new OperationWithAction(OperationCode.QUERY),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all keys using one key at a time
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3,
            OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all keys using one key at a time
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2,
            OpFlags.USE_OLDCONN, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Test GET_ENTRY inside a TX, see #49951
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_GET_ENTRY_IN_TX | OpFlags.CHECK_FAIL, 4),

        OperationWithAction.OPBLOCK_END };

    runOpsWithFailover(allOps, "testAllOpsWithFailover");
  }

  // End Region: Tests

  @Override
  protected final void preTearDown() throws Exception {
    // close the clients first
    client1.invoke(SecurityTestUtil.class, "closeCache");
    client2.invoke(SecurityTestUtil.class, "closeCache");
    SecurityTestUtil.closeCache();
    // then close the servers
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");
  }
}
