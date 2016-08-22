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
package com.gemstone.gemfire.security;

import static com.gemstone.gemfire.internal.AvailablePort.*;
import static com.gemstone.gemfire.security.SecurityTestUtils.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.IgnoredException.*;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.security.generator.AuthzCredentialGenerator;
import com.gemstone.gemfire.security.generator.CredentialGenerator;
import com.gemstone.gemfire.security.generator.DummyCredentialGenerator;
import com.gemstone.gemfire.security.generator.XmlAuthzCredentialGenerator;
import com.gemstone.gemfire.security.templates.UserPasswordAuthInit;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

/**
 * Tests for authorization from client to server. This tests for authorization
 * of all operations with both valid and invalid credentials/modules with
 * pre-operation callbacks. It also checks for authorization in case of
 * failover.
 *
 * @since GemFire 5.5
 */
@Category({ DistributedTest.class, SecurityTest.class })
public class ClientAuthorizationDUnitTest extends ClientAuthorizationTestCase {

  @Override
  public final void preTearDownClientAuthorizationTestBase() throws Exception {
    closeCache();
  }

  @Test
  public void testAllowPutsGets() {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    getLogWriter().info("testAllowPutsGets: Using authinit: " + authInit);
    getLogWriter().info("testAllowPutsGets: Using authenticator: " + authenticator);
    getLogWriter().info("testAllowPutsGets: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    int port1 = createServer1(javaProps, serverProps);
    int port2 = createServer2(javaProps, serverProps);

    // Start client1 with valid CREATE credentials
    Properties createCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.PUT }, new String[] { regionName }, 1);
    javaProps = cGen.getJavaProperties();

    getLogWriter().info("testAllowPutsGets: For first client credentials: " + createCredentials);

    createClient1NoException(javaProps, authInit, port1, port2, createCredentials);

    // Start client2 with valid GET credentials
    Properties getCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.GET }, new String[] { regionName }, 2);
    javaProps = cGen.getJavaProperties();

    getLogWriter().info("testAllowPutsGets: For second client credentials: " + getCredentials);

    createClient2NoException(javaProps, authInit, port1, port2, getCredentials);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2, NO_EXCEPTION));

    // Verify that the gets succeed
    client2.invoke(() -> doGets(2, NO_EXCEPTION));
  }

  @Test
  public void testPutAllWithSecurity() {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    getLogWriter().info("testPutAllWithSecurity: Using authinit: " + authInit);
    getLogWriter().info("testPutAllWithSecurity: Using authenticator: " + authenticator);
    getLogWriter().info("testPutAllWithSecurity: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    int port1 = createServer1(javaProps, serverProps);
    int port2 = createServer2(javaProps, serverProps);

    // Start client1 with valid CREATE credentials
    Properties createCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.PUTALL }, new String[] { regionName }, 1);
    javaProps = cGen.getJavaProperties();

    getLogWriter().info("testPutAllWithSecurity: For first client credentials: " + createCredentials);

    createClient1NoException(javaProps, authInit, port1, port2, createCredentials);

    // Perform some put all operations from client1
    client1.invoke(() -> doPutAllP());
  }

  @Test
  public void testDisallowPutsGets() {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    getLogWriter().info("testDisallowPutsGets: Using authinit: " + authInit);
    getLogWriter().info("testDisallowPutsGets: Using authenticator: " + authenticator);
    getLogWriter().info("testDisallowPutsGets: Using accessor: " + accessor);

    // Check that we indeed can obtain valid credentials not allowed to do gets
    Properties createCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.PUT }, new String[] { regionName }, 1);
    Properties createJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testDisallowPutsGets: For first client credentials: " + createCredentials);

    Properties getCredentials = gen.getDisallowedCredentials(new OperationCode[] { OperationCode.GET }, new String[] { regionName }, 2);
    Properties getJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testDisallowPutsGets: For second client disallowed GET credentials: " + getCredentials);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    int port1 = createServer1(javaProps, serverProps);
    int port2 = createServer2(javaProps, serverProps);

    createClient1NoException(createJavaProps, authInit, port1, port2, createCredentials);

    createClient2NoException(getJavaProps, authInit, port1, port2, getCredentials);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2, NO_EXCEPTION));

    // Gets as normal user should throw exception
    client2.invoke(() -> doGets(2, NOTAUTHZ_EXCEPTION));

    // Try to connect client2 with reader credentials
    getCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.GET }, new String[] { regionName }, 5);
    getJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testDisallowPutsGets: For second client with GET credentials: " + getCredentials);

    createClient2NoException(getJavaProps, authInit, port1, port2, getCredentials);

    // Verify that the gets succeed
    client2.invoke(() -> doGets(2, NO_EXCEPTION));

    // Verify that the puts throw exception
    client2.invoke(() -> doNPuts(2, NOTAUTHZ_EXCEPTION));
  }

  @Test
  public void testInvalidAccessor() {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    getLogWriter().info("testInvalidAccessor: Using authinit: " + authInit);
    getLogWriter().info("testInvalidAccessor: Using authenticator: " + authenticator);

    // Start server1 with invalid accessor
    Properties serverProps = buildProperties(authenticator, "com.gemstone.none", false, extraAuthProps, extraAuthzProps);

    int port1 = createServer1(javaProps, serverProps);
    int port2 = getRandomAvailablePort(SOCKET);

    // Client creation should throw exceptions
    Properties createCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.PUT }, new String[] { regionName }, 3);
    Properties createJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testInvalidAccessor: For first client CREATE credentials: " + createCredentials);

    Properties getCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.GET }, new String[] { regionName }, 7);
    Properties getJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testInvalidAccessor: For second client GET credentials: " + getCredentials);

    client1.invoke(() -> ClientAuthenticationTestUtils.createCacheClient( authInit, createCredentials, createJavaProps, port1, port2, 0, false, false, NO_EXCEPTION));
    client1.invoke(() -> doPuts(1, AUTHFAIL_EXCEPTION));

    client2.invoke(() -> ClientAuthenticationTestUtils.createCacheClient( authInit, getCredentials, getJavaProps, port1, port2, 0, false, false, NO_EXCEPTION));
    client2.invoke(() -> doPuts(1, AUTHFAIL_EXCEPTION));

    // Now start server2 that has valid accessor
    getLogWriter().info("testInvalidAccessor: Using accessor: " + accessor);
    serverProps = buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);
    createServer2(javaProps, serverProps, port2);
    server1.invoke(() -> closeCache());

    createClient1NoException(createJavaProps, authInit, port1, port2, createCredentials);
    createClient2NoException(getJavaProps, authInit, port1, port2, getCredentials);

    // Now perform some put operations from client1
    client1.invoke(() -> doPuts(4, NO_EXCEPTION));

    // Verify that the gets succeed
    client2.invoke(() -> doGets(4, NO_EXCEPTION));
  }

  @Test
  public void testPutsGetsWithFailover() {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    getLogWriter().info("testPutsGetsWithFailover: Using authinit: " + authInit);
    getLogWriter().info("testPutsGetsWithFailover: Using authenticator: " + authenticator);
    getLogWriter().info("testPutsGetsWithFailover: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    int port1 = createServer1(javaProps, serverProps);

    // Get a port for second server but do not start it. This forces the clients to connect to the first server
    int port2 = getRandomAvailablePort(SOCKET);

    // Start client1 with valid CREATE credentials
    Properties createCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.PUT }, new String[] { regionName }, 1);
    Properties createJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testPutsGetsWithFailover: For first client credentials: " + createCredentials);

    createClient1NoException(createJavaProps, authInit, port1, port2, createCredentials);

    // Start client2 with valid GET credentials
    Properties getCredentials = gen.getAllowedCredentials(new OperationCode[] { OperationCode.GET }, new String[] { regionName }, 5);
    Properties getJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testPutsGetsWithFailover: For second client credentials: " + getCredentials);

    createClient2NoException(getJavaProps, authInit, port1, port2, getCredentials);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2, NO_EXCEPTION));

    // Verify that the puts succeeded
    client2.invoke(() -> doGets(2, NO_EXCEPTION));

    createServer2(javaProps, serverProps, port2);
    server1.invoke(() -> closeCache());

    // Perform some put operations from client1
    client1.invoke(() -> doNPuts(4, NO_EXCEPTION));

    // Verify that the puts succeeded
    client2.invoke(() -> doNGets(4, NO_EXCEPTION));

    // Now re-connect with credentials not allowed to do gets
    Properties noGetCredentials = gen.getDisallowedCredentials(new OperationCode[] { OperationCode.GET }, new String[] { regionName }, 9);
    getJavaProps = cGen.getJavaProperties();

    getLogWriter().info("testPutsGetsWithFailover: For second client disallowed GET credentials: " + noGetCredentials);

    createClient2NoException(getJavaProps, authInit, port1, port2, noGetCredentials);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(4, NO_EXCEPTION));

    // Gets as normal user should throw exception
    client2.invoke(() -> doGets(4, NOTAUTHZ_EXCEPTION));

    // force a failover and do the drill again
    server1.invoke(() -> ClientAuthorizationTestCase.createCacheServer( getLocatorPort(), port1, serverProps, javaProps ));
    server2.invoke(() -> closeCache());

    // Perform some put operations from client1
    client1.invoke(() -> doNPuts(4, NO_EXCEPTION));

    // Gets as normal user should throw exception
    client2.invoke(() -> doNGets(4, NOTAUTHZ_EXCEPTION));

    createClient2NoException(getJavaProps, authInit, port1, port2, getCredentials);

    // Verify that the gets succeed
    client2.invoke(() -> doNGets(4, NO_EXCEPTION));

    // Verify that the puts throw exception
    client2.invoke(() -> doPuts(4, NOTAUTHZ_EXCEPTION));
  }

  @Test
  public void testUnregisterInterestWithFailover() throws InterruptedException {
    OperationWithAction[] unregisterOps = unregisterOpsForTestUnregisterInterestWithFailover();

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

    getLogWriter().info("testAllOpsWithFailover: Using authinit: " + authInit);
    getLogWriter().info("testAllOpsWithFailover: Using authenticator: " + authenticator);
    getLogWriter().info("testAllOpsWithFailover: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    // Get ports for the servers
    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = randomAvailableTCPPorts[0];
    int port2 = randomAvailableTCPPorts[1];

    // Perform all the ops on the clients
    List opBlock = new ArrayList();
    for (int opNum = 0; opNum < unregisterOps.length; ++opNum) {

      // Start client with valid credentials as specified in OperationWithAction
      OperationWithAction currentOp = unregisterOps[opNum];
      if (currentOp.equals(OperationWithAction.OPBLOCK_END) || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {

        // End of current operation block; execute all the operations on the servers with/without failover
        if (opBlock.size() > 0) {
          // Start the first server and execute the operation block
          server1.invoke(() -> ClientAuthorizationTestCase.createCacheServer(getLocatorPort(), port1, serverProps, javaProps));
          server2.invoke(() -> closeCache());

          executeRIOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, javaProps);

          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            createServer2(javaProps, serverProps, port2);
            server1.invoke(() -> closeCache());

            executeRIOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, javaProps);
          }
          opBlock.clear();
        }

      } else {
        currentOp.setOpNum(opNum);
        opBlock.add(currentOp);
      }
    }
  }

  @Test
  public void testAllOpsWithFailover() throws InterruptedException {
    addIgnoredException("Read timed out");
    runOpsWithFailOver(allOpsForAllOpsWithFailover(), "testAllOpsWithFailover");
  }

  private OperationWithAction[] unregisterOpsForTestUnregisterInterestWithFailover() {
    return new OperationWithAction[] {
        // Register interest in all KEYS using one key at a time
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.UNREGISTER_INTEREST, 3, OpFlags.NONE, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using one key at a time
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_OLDCONN | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2, OpFlags.USE_OLDCONN, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all KEYS using list
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_LIST, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 1, OpFlags.USE_LIST, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT, 2),
        new OperationWithAction(OperationCode.GET, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using list
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_OLDCONN | OpFlags.USE_LIST | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 1, OpFlags.USE_OLDCONN | OpFlags.USE_LIST, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all KEYS using regular expression
        new OperationWithAction(OperationCode.REGISTER_INTEREST, OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_REGEX, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2, OpFlags.USE_REGEX, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using regular expression
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 3, OpFlags.USE_OLDCONN | OpFlags.USE_REGEX | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2, OpFlags.USE_OLDCONN | OpFlags.USE_REGEX, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END
    };
  }

  private OperationWithAction[] allOpsForAllOpsWithFailover() {
    return new OperationWithAction[] {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY, 4),

        // OPBLOCK_END indicates end of an operation block; the above block of three operations will be first executed on server1 and then on server2 after failover
        OperationWithAction.OPBLOCK_END,

        // Test PUTALL and verify with GETs
        new OperationWithAction(OperationCode.PUTALL, 3, OpFlags.USE_NEWVAL | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUTALL, 1, OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_NEWVAL | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),

        OperationWithAction.OPBLOCK_END,

        // Test DESTROY and verify with a GET and that key should not exist
        new OperationWithAction(OperationCode.DESTROY, 3, OpFlags.USE_NEWVAL | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.DESTROY),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL, 4), // bruce: added check_nokey because we now bring tombstones to the client in 8.0
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_NEWVAL, 4),

        OperationWithAction.OPBLOCK_END,

        // Check CONTAINS_KEY
        new OperationWithAction(OperationCode.CONTAINS_KEY, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.CONTAINS_KEY),
        // Destroy the KEYS and check for failure in CONTAINS_KEY
        new OperationWithAction(OperationCode.DESTROY, 2),
        new OperationWithAction(OperationCode.CONTAINS_KEY, 3, OpFlags.CHECK_FAIL | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.CONTAINS_KEY, 1, OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL, 4),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT),

        OperationWithAction.OPBLOCK_END,

        // Check KEY_SET
        new OperationWithAction(OperationCode.KEY_SET, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.KEY_SET, 2),

        OperationWithAction.OPBLOCK_END,

        // Check QUERY
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.QUERY),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all KEYS using one key at a time
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using one key at a time
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2, OpFlags.USE_OLDCONN, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Test GET_ENTRY inside a TX, see #49951
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_GET_ENTRY_IN_TX | OpFlags.CHECK_FAIL, 4),

        OperationWithAction.OPBLOCK_END };
  }

  private Properties getUserPassword(final String userName) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
    return props;
  }

  private void executeRIOpBlock(final List<OperationWithAction> opBlock, final int port1, final int port2, final String authInit, final Properties extraAuthProps, final Properties extraAuthzProps, final Properties javaProps) throws InterruptedException {
    for (Iterator opIter = opBlock.iterator(); opIter.hasNext();) {
      // Start client with valid credentials as specified in OperationWithAction
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

      getLogWriter().info( "executeRIOpBlock: performing operation number [" + currentOp.getOpNum() + "]: " + currentOp);
      if ((opFlags & OpFlags.USE_OLDCONN) == 0) {
        Properties opCredentials = null;
        String currentRegionName = '/' + regionName;
        if ((opFlags & OpFlags.USE_SUBREGION) > 0) {
          currentRegionName += ('/' + SUBREGION_NAME);
        }
        String credentialsTypeStr;
        OperationCode authOpCode = currentOp.getAuthzOperationCode();

        if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0 || (opFlags & OpFlags.USE_NOTAUTHZ) > 0 || !authOpCode.equals(opCode)) {
          credentialsTypeStr = " unauthorized " + authOpCode;
          if (authOpCode.isRegisterInterest()) {
            opCredentials = getUserPassword("reader7");
          } else if (authOpCode.isUnregisterInterest()) {
            opCredentials = getUserPassword("reader6");
          } else {
            fail("executeRIOpBlock: cannot determine credentials for" + credentialsTypeStr);
          }

        } else {
          credentialsTypeStr = " authorized " + authOpCode;
          if (authOpCode.isRegisterInterest() || authOpCode.isUnregisterInterest()) {
            opCredentials = getUserPassword("reader5");
          } else if (authOpCode.isPut()) {
            opCredentials = getUserPassword("writer1");
          } else if (authOpCode.isGet()) {
            opCredentials = getUserPassword("reader1");
          } else {
            fail("executeRIOpBlock: cannot determine credentials for" + credentialsTypeStr);
          }
        }

        Properties clientProps = concatProperties(new Properties[] { opCredentials, extraAuthProps, extraAuthzProps });

        // Start the client with valid credentials but allowed or disallowed to perform an operation
        getLogWriter().info("executeRIOpBlock: For client" + clientNum + credentialsTypeStr + " credentials: " + opCredentials);
        if (useThisVM) {
          createCacheClientWithDynamicRegion(authInit, clientProps, javaProps, new int[] { port1, port2 }, 0, false, NO_EXCEPTION);
        } else {
          clientVM.invoke(() -> createCacheClient(authInit, clientProps, javaProps, new int[] { port1, port2 }, 0, false, NO_EXCEPTION));
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
        doOp(opCode, currentOp.getIndices(), opFlags, expectedResult);

      } else {
        int[] indices = currentOp.getIndices();
        clientVM.invoke(() -> ClientAuthorizationTestCase.doOp(opCode, indices, opFlags, expectedResult));
      }
    }
  }

  private void createClient2NoException(final Properties javaProps, final String authInit, final int port1, final int port2, final Properties getCredentials) {
    client2.invoke(() -> ClientAuthenticationTestUtils.createCacheClient(authInit, getCredentials, javaProps, port1, port2, 0, NO_EXCEPTION));
  }

  private void createClient1NoException(final Properties javaProps, final String authInit, final int port1, final int port2, final Properties createCredentials) {
    client1.invoke(() -> ClientAuthenticationTestUtils.createCacheClient(authInit, createCredentials, javaProps, port1, port2, 0, NO_EXCEPTION));
  }

  private int createServer2(final Properties javaProps, final Properties serverProps) {
    return server2.invoke(() -> ClientAuthorizationTestCase.createCacheServer(getLocatorPort(), serverProps, javaProps));
  }

  private int createServer1(final Properties javaProps, final Properties serverProps) {
    return server1.invoke(() -> ClientAuthorizationTestCase.createCacheServer(getLocatorPort(), serverProps, javaProps));
  }

  private void createServer2(Properties javaProps, Properties serverProps, int port2) {
    server2.invoke(() -> ClientAuthorizationTestCase.createCacheServer(getLocatorPort(), port2, serverProps, javaProps));
  }
}
