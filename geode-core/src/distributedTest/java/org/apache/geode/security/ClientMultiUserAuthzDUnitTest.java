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
import static org.apache.geode.security.SecurityTestUtils.REGION_NAME;
import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.security.SecurityTestUtils.createCacheClient;
import static org.apache.geode.security.SecurityTestUtils.createCacheClientForMultiUserMode;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserContainsKeys;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserDestroys;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserFE;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserGetAll;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserGets;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserInvalidates;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserPuts;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserQueries;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserQueryExecute;
import static org.apache.geode.security.SecurityTestUtils.doMultiUserRegionDestroys;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.execute.PRClientServerTestBase;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class ClientMultiUserAuthzDUnitTest extends ClientAuthorizationTestCase {

  @Override
  public final void preTearDownClientAuthorizationTestBase() throws Exception {
    closeCache();
  }

  /**
   * Tests with one user authorized to do puts/gets/containsKey/destroys and another not authorized
   * for the same.
   */
  @Test
  public void testOps1() throws Exception {
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

      getLogWriter().info("testOps1: Using authinit: " + authInit);
      getLogWriter().info("testOps1: Using authenticator: " + authenticator);
      getLogWriter().info("testOps1: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps =
          buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

      int port1 = createCacheServerOnVM(server1, javaProps, serverProps);
      int port2 = createCacheServerOnVM(server2, javaProps, serverProps);

      if (!prepareClientsForOps(gen, cGen,
          new OperationCode[] {OperationCode.PUT, OperationCode.PUT},
          new OperationCode[] {OperationCode.GET, OperationCode.GET}, javaProps, authInit, port1,
          port2)) {
        continue;
      }

      verifyPutsGets();

      if (!prepareClientsForOps(gen, cGen,
          new OperationCode[] {OperationCode.PUT, OperationCode.CONTAINS_KEY},
          new OperationCode[] {OperationCode.DESTROY, OperationCode.DESTROY}, javaProps, authInit,
          port1, port2)) {
        continue;
      }

      verifyContainsKeyDestroys();

      if (!prepareClientsForOps(gen, cGen,
          new OperationCode[] {OperationCode.PUT, OperationCode.CONTAINS_KEY},
          new OperationCode[] {OperationCode.INVALIDATE, OperationCode.INVALIDATE}, javaProps,
          authInit, port1, port2)) {
        continue;
      }

      verifyContainsKeyInvalidates();

      if (!prepareClientsForOps(gen, cGen,
          new OperationCode[] {OperationCode.GET, OperationCode.GET},
          new OperationCode[] {OperationCode.REGION_DESTROY, OperationCode.REGION_DESTROY},
          javaProps, authInit, port1, port2)) {
        continue;
      }

      verifyGetAllInTX();
      verifyGetAllRegionDestroys();
    }
  }

  /**
   * Test query/function execute
   */
  @Test
  public void testOps2() throws Exception {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();

    getLogWriter().info("testOps2: Using authinit: " + authInit);
    getLogWriter().info("testOps2: Using authenticator: " + authenticator);
    getLogWriter().info("testOps2: Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps =
        buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    int port1 = createCacheServerOnVM(server1, javaProps, serverProps);
    int port2 = createCacheServerOnVM(server2, javaProps, serverProps);

    // Start client1 with valid/invalid QUERY credentials
    Properties[] client1Credentials = new Properties[] {
        gen.getAllowedCredentials(new OperationCode[] {OperationCode.PUT, OperationCode.QUERY},
            new String[] {regionName}, 1),
        gen.getDisallowedCredentials(new OperationCode[] {OperationCode.PUT, OperationCode.QUERY},
            new String[] {regionName}, 1)};

    javaProps = cGen.getJavaProperties();
    getLogWriter().info("testOps2: For first client credentials: " + client1Credentials[0] + "\n"
        + client1Credentials[1]);

    final Properties finalJavaProps = javaProps;
    client1.invoke(() -> createCacheClientForMultiUserMode(2, authInit, client1Credentials,
        finalJavaProps, new int[] {port1, port2}, -1, false, NO_EXCEPTION));

    // Start client2 with valid/invalid EXECUTE_FUNCTION credentials
    Properties[] client2Credentials = new Properties[] {
        gen.getAllowedCredentials(new OperationCode[] {OperationCode.EXECUTE_FUNCTION},
            new String[] {regionName}, 2),
        gen.getDisallowedCredentials(new OperationCode[] {OperationCode.EXECUTE_FUNCTION},
            new String[] {regionName}, 9)};

    javaProps = cGen.getJavaProperties();
    getLogWriter().info("testOps2: For second client credentials: " + client2Credentials[0] + "\n"
        + client2Credentials[1]);

    final Properties finalJavaProps2 = javaProps;
    client2.invoke(() -> createCacheClientForMultiUserMode(2, authInit, client2Credentials,
        finalJavaProps2, new int[] {port1, port2}, -1, false, NO_EXCEPTION));

    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);

    server1.invoke(() -> PRClientServerTestBase.registerFunction(function));

    server2.invoke(() -> PRClientServerTestBase.registerFunction(function));

    // Perform some put operations before verifying queries
    client1.invoke(() -> doMultiUserPuts(4, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));
    client1.invoke(() -> doMultiUserQueries(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, 4));
    client1
        .invoke(() -> doMultiUserQueryExecute(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, 4));

    // Verify that the FE succeeds/fails
    client2.invoke(
        () -> doMultiUserFE(2, function, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, false));

    // Failover
    server1.invoke(() -> closeCache());
    Thread.sleep(2000);

    client1.invoke(() -> doMultiUserPuts(4, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));

    client1.invoke(() -> doMultiUserQueries(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, 4));
    client1
        .invoke(() -> doMultiUserQueryExecute(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, 4));

    // Verify that the FE succeeds/fails
    client2.invoke(
        () -> doMultiUserFE(2, function, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, true));
  }

  @Test
  public void testOpsWithClientsInDifferentModes() throws Exception {
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

      getLogWriter().info("testOpsWithClientsInDifferentModes: Using authinit: " + authInit);
      getLogWriter()
          .info("testOpsWithClientsInDifferentModes: Using authenticator: " + authenticator);
      getLogWriter().info("testOpsWithClientsInDifferentModes: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps =
          buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

      int port1 = createCacheServerOnVM(server1, javaProps, serverProps);
      int port2 = createCacheServerOnVM(server2, javaProps, serverProps);

      if (!prepareClientsForOps(gen, cGen,
          new OperationCode[] {OperationCode.PUT, OperationCode.PUT},
          new OperationCode[] {OperationCode.GET, OperationCode.GET}, javaProps, authInit, port1,
          port2, false, true)) {
        continue;
      }

      verifyPutsGets(false, true);

      if (!prepareClientsForOps(gen, cGen,
          new OperationCode[] {OperationCode.PUT, OperationCode.CONTAINS_KEY},
          new OperationCode[] {OperationCode.DESTROY, OperationCode.DESTROY}, javaProps, authInit,
          port1, port2, false, false)) {
        continue;
      }

      verifyContainsKeyDestroys(false, false);
    }
  }

  private boolean prepareClientsForOps(final AuthzCredentialGenerator gen,
      final CredentialGenerator cGen, final OperationCode[] client1OpCodes,
      final OperationCode[] client2OpCodes, final Properties javaProps, final String authInit,
      final int port1, final int port2) {
    return prepareClientsForOps(gen, cGen, client1OpCodes, client2OpCodes, javaProps, authInit,
        port1, port2, true /* both clients in multiuser mode */, false /* unused */);
  }

  private boolean prepareClientsForOps(final AuthzCredentialGenerator gen,
      final CredentialGenerator cGen, final OperationCode[] client1OpCodes,
      final OperationCode[] client2OpCodes, Properties javaProps, final String authInit,
      final int port1, final int port2, final boolean bothClientsInMultiuserMode,
      final boolean allowOp) {
    // Start client1 with valid/invalid client1OpCodes credentials
    Properties[] client1Credentials =
        new Properties[] {gen.getAllowedCredentials(client1OpCodes, new String[] {regionName}, 1),
            gen.getDisallowedCredentials(new OperationCode[] {client1OpCodes[1]},
                new String[] {regionName}, 1)};

    if (client1Credentials[0] == null || client1Credentials[0].size() == 0) {
      getLogWriter().info("testOps1: Unable to obtain valid credentials with "
          + client1OpCodes[0].toString() + " permission; skipping this combination.");
      return false;
    }

    if (client1Credentials[1] == null || client1Credentials[1].size() == 0) {
      getLogWriter().info("testOps1: Unable to obtain valid credentials with no "
          + client1OpCodes[0].toString() + " permission; skipping this combination.");
      return false;
    }

    javaProps = cGen.getJavaProperties();
    getLogWriter().info("testOps1: For first client credentials: " + client1Credentials[0] + "\n"
        + client1Credentials[1]);
    final Properties finalJavaProps = javaProps;

    client1.invoke(() -> createCacheClientForMultiUserMode(2, authInit, client1Credentials,
        finalJavaProps, new int[] {port1, port2}, -1, false, NO_EXCEPTION));

    // Start client2 with valid/invalid client2OpCodes credentials
    Properties[] client2Credentials =
        new Properties[] {gen.getAllowedCredentials(client2OpCodes, new String[] {regionName}, 2),
            gen.getDisallowedCredentials(client2OpCodes, new String[] {regionName}, 9)};

    if (client2Credentials[0] == null || client2Credentials[0].size() == 0) {
      getLogWriter().info("testOps1: Unable to obtain valid credentials with "
          + client2OpCodes[0].toString() + " permission; skipping this combination.");
      return false;
    }

    if (client2Credentials[1] == null || client2Credentials[1].size() == 0) {
      getLogWriter().info("testOps1: Unable to obtain valid credentials with no "
          + client2OpCodes[0].toString() + " permission; skipping this combination.");
      return false;
    }

    javaProps = cGen.getJavaProperties();
    getLogWriter().info("testOps1: For second client credentials: " + client2Credentials[0] + "\n"
        + client2Credentials[1]);

    if (bothClientsInMultiuserMode) {
      final Properties finalJavaProps2 = javaProps;
      client2.invoke(() -> createCacheClientForMultiUserMode(2, authInit, client2Credentials,
          finalJavaProps2, new int[] {port1, port2}, -1, false, NO_EXCEPTION));

    } else {
      int credentialsIndex = allowOp ? 0 : 1;
      final Properties finalJavaProps2 = javaProps;
      client2.invoke(() -> createCacheClient(authInit, client2Credentials[credentialsIndex],
          finalJavaProps2, new int[] {port1, port2}, -1, false, false, NO_EXCEPTION));
    }

    return true;
  }

  private void verifyPutsGets() throws Exception {
    verifyPutsGets(true, false /* unused */);
  }

  private void verifyPutsGets(final boolean isMultiuser, final boolean opAllowed) throws Exception {
    // Perform some put operations from client1
    client1.invoke(() -> doMultiUserPuts(2, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));

    // Verify that the gets succeed/fail
    if (isMultiuser) {
      client2.invoke(() -> doMultiUserGets(2, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));

    } else {
      int expectedResult = (opAllowed) ? NO_EXCEPTION : NOTAUTHZ_EXCEPTION;
      client2.invoke(() -> doMultiUserGets(1, 1, new int[] {expectedResult}));
    }
  }

  private void verifyContainsKeyDestroys() throws Exception {
    verifyContainsKeyDestroys(true, false /* unused */);
  }

  private void verifyContainsKeyDestroys(final boolean isMultiUser, final boolean opAllowed)
      throws Exception {
    // Do puts before verifying containsKey
    client1.invoke(() -> doMultiUserPuts(2, 2, new int[] {NO_EXCEPTION, NO_EXCEPTION}));
    client1.invoke(() -> doMultiUserContainsKeys(1, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION},
        new boolean[] {true, false}));

    // Verify that the destroys succeed/fail
    if (isMultiUser) {
      client2.invoke(() -> doMultiUserDestroys(2, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));

    } else {
      int expectedResult = (opAllowed) ? NO_EXCEPTION : NOTAUTHZ_EXCEPTION;
      client2.invoke(() -> doMultiUserDestroys(1, 1, new int[] {expectedResult}));
    }
  }

  private void verifyContainsKeyInvalidates() throws Exception {
    verifyContainsKeyInvalidates(true, false /* unused */);
  }

  private void verifyContainsKeyInvalidates(final boolean isMultiUser, final boolean opAllowed)
      throws Exception {
    // Do puts before verifying containsKey
    client1.invoke(() -> doMultiUserPuts(2, 2, new int[] {NO_EXCEPTION, NO_EXCEPTION}));
    client1.invoke(() -> doMultiUserContainsKeys(1, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION},
        new boolean[] {true, false}));

    // Verify that the invalidates succeed/fail
    if (isMultiUser) {
      client2
          .invoke(() -> doMultiUserInvalidates(2, 2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));

    } else {
      int expectedResult = (opAllowed) ? NO_EXCEPTION : NOTAUTHZ_EXCEPTION;
      client2.invoke(() -> doMultiUserInvalidates(1, 1, new int[] {expectedResult}));
    }
  }

  private void verifyGetAllInTX() {
    server1.invoke(() -> doPuts());
    client1.invoke(
        () -> doMultiUserGetAll(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}, true/* use TX */));
  }

  private void verifyGetAllRegionDestroys() {
    server1.invoke(() -> doPuts());
    client1.invoke(() -> doMultiUserGetAll(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));

    // Verify that the region destroys succeed/fail
    client2
        .invoke(() -> doMultiUserRegionDestroys(2, new int[] {NO_EXCEPTION, NOTAUTHZ_EXCEPTION}));
  }

  private void doPuts() {
    Region region = GemFireCacheImpl.getInstance().getRegion(REGION_NAME);
    region.put("key1", "value1");
    region.put("key2", "value2");
  }

  private int createCacheServerOnVM(final VM server, final Properties javaProps,
      final Properties serverProps) {
    return server
        .invoke(() -> ClientAuthorizationTestCase.createCacheServer(serverProps, javaProps));
  }
}
