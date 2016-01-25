
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


import java.util.Iterator;
import java.util.Properties;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.execute.PRClientServerTestBase;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.test.dunit.Host;

public class ClientMultiUserAuthzDUnitTest extends ClientAuthorizationTestBase {

  /** constructor */
  public ClientMultiUserAuthzDUnitTest(String name) {
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

  // Tests with one user authorized to do puts/gets/containsKey/destroys and
  // another not authorized for the same.
  public void testOps1() throws Exception {
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

      getLogWriter().info("testOps1: Using authinit: " + authInit);
      getLogWriter().info(
          "testOps1: Using authenticator: " + authenticator);
      getLogWriter().info("testOps1: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Integer port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      Integer port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      server1.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] {SecurityTestUtil.getLocatorPort(), port1, serverProps,
              javaProps});
      server2.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] {SecurityTestUtil.getLocatorPort(), port2, serverProps,
              javaProps});

      if (!prepareClientsForOps(gen, cGen, new OperationCode[] {
          OperationCode.PUT, OperationCode.PUT}, new OperationCode[] {
          OperationCode.GET, OperationCode.GET}, javaProps, authInit, port1,
          port2)) {
        continue;
      }
      verifyPutsGets();

      if (!prepareClientsForOps(gen, cGen, new OperationCode[] {
          OperationCode.PUT, OperationCode.CONTAINS_KEY}, new OperationCode[] {
          OperationCode.DESTROY, OperationCode.DESTROY},
          javaProps, authInit, port1, port2)) {
        continue;
      }
      verifyContainsKeyDestroys();

      if (!prepareClientsForOps(gen, cGen, new OperationCode[] {
          OperationCode.PUT, OperationCode.CONTAINS_KEY}, new OperationCode[] {
          OperationCode.INVALIDATE, OperationCode.INVALIDATE},
          javaProps, authInit, port1, port2)) {
        continue;
      }
      verifyContainsKeyInvalidates();

      if (!prepareClientsForOps(gen, cGen, new OperationCode[] {
          OperationCode.GET, OperationCode.GET}, new OperationCode[] {
          OperationCode.REGION_DESTROY, OperationCode.REGION_DESTROY},
          javaProps, authInit, port1, port2)) {
        continue;
      }
      verifyGetAllInTX();
      verifyGetAllRegionDestroys();
    }
  }

  private boolean prepareClientsForOps(AuthzCredentialGenerator gen,
      CredentialGenerator cGen, OperationCode[] client1OpCodes,
      OperationCode[] client2OpCodes, Properties javaProps, String authInit,
      Integer port1, Integer port2) {
    return prepareClientsForOps(gen, cGen, client1OpCodes, client2OpCodes,
        javaProps, authInit, port1, port2, Boolean.TRUE /*
                                                         * both clients in
                                                         * multiuser mode
                                                         */, Boolean.FALSE /* unused */);
  }

  private boolean prepareClientsForOps(AuthzCredentialGenerator gen,
      CredentialGenerator cGen, OperationCode[] client1OpCodes,
      OperationCode[] client2OpCodes, Properties javaProps, String authInit,
      Integer port1, Integer port2, Boolean bothClientsInMultiuserMode,
      Boolean allowOp) {
    // Start client1 with valid/invalid client1OpCodes credentials
    Properties[] client1Credentials = new Properties[] {
        gen.getAllowedCredentials(client1OpCodes, new String[] {regionName}, 1),
        gen.getDisallowedCredentials(new OperationCode[] {client1OpCodes[1]},
            new String[] {regionName}, 1)};
    if (client1Credentials[0] == null || client1Credentials[0].size() == 0) {
      getLogWriter().info(
          "testOps1: Unable to obtain valid credentials with "
              + client1OpCodes[0].toString()
              + " permission; skipping this combination.");
      return false;
    }
    if (client1Credentials[1] == null || client1Credentials[1].size() == 0) {
      getLogWriter().info(
          "testOps1: Unable to obtain valid credentials with no "
              + client1OpCodes[0].toString()
              + " permission; skipping this combination.");
      return false;
    }
    javaProps = cGen.getJavaProperties();
    getLogWriter().info(
        "testOps1: For first client credentials: " + client1Credentials[0]
            + "\n" + client1Credentials[1]);
    client1.invoke(SecurityTestUtil.class, "createCacheClientForMultiUserMode",
        new Object[] {Integer.valueOf(2), authInit, client1Credentials,
            javaProps, new Integer[] {port1, port2}, null, Boolean.FALSE,
            SecurityTestUtil.NO_EXCEPTION});

    // Start client2 with valid/invalid client2OpCodes credentials
    Properties[] client2Credentials = new Properties[] {
        gen.getAllowedCredentials(client2OpCodes,
            new String[] {regionName}, 2),
        gen.getDisallowedCredentials(client2OpCodes,
            new String[] {regionName}, 9)};
    if (client2Credentials[0] == null || client2Credentials[0].size() == 0) {
      getLogWriter().info(
          "testOps1: Unable to obtain valid credentials with "
              + client2OpCodes[0].toString()
              + " permission; skipping this combination.");
      return false;
    }
    if (client2Credentials[1] == null || client2Credentials[1].size() == 0) {
      getLogWriter().info(
          "testOps1: Unable to obtain valid credentials with no "
              + client2OpCodes[0].toString()
              + " permission; skipping this combination.");
      return false;
    }
    javaProps = cGen.getJavaProperties();
    getLogWriter().info(
        "testOps1: For second client credentials: " + client2Credentials[0]
            + "\n" + client2Credentials[1]);
    if (bothClientsInMultiuserMode) {
      client2.invoke(SecurityTestUtil.class,
          "createCacheClientForMultiUserMode", new Object[] {
              Integer.valueOf(2), authInit, client2Credentials, javaProps,
              new Integer[] {port1, port2}, null, Boolean.FALSE,
              SecurityTestUtil.NO_EXCEPTION});
    } else {
      int credentialsIndex = allowOp ? 0 : 1;
      client2.invoke(SecurityTestUtil.class, "createCacheClient", new Object[] {
          authInit, client2Credentials[credentialsIndex], javaProps,
          new Integer[] {port1, port2}, null, Boolean.FALSE, "false",
          SecurityTestUtil.NO_EXCEPTION});
    }
    return true;
  }

  private void verifyPutsGets() throws Exception {
    verifyPutsGets(true, false /*unused */);
  }

  private void verifyPutsGets(Boolean isMultiuser, Boolean opAllowed)
      throws Exception {
    // Perform some put operations from client1
    client1.invoke(SecurityTestUtil.class, "doMultiUserPuts", new Object[] {
        Integer.valueOf(2),
        Integer.valueOf(2),
        new Integer[] {SecurityTestUtil.NO_EXCEPTION,
            SecurityTestUtil.NOTAUTHZ_EXCEPTION}});

    // Verify that the gets succeed/fail
    if (isMultiuser) {
    client2.invoke(SecurityTestUtil.class, "doMultiUserGets", new Object[] {
        Integer.valueOf(2),
        Integer.valueOf(2),
        new Integer[] {SecurityTestUtil.NO_EXCEPTION,
            SecurityTestUtil.NOTAUTHZ_EXCEPTION}});
    } else {
      int expectedResult = (opAllowed) ? SecurityTestUtil.NO_EXCEPTION
          : SecurityTestUtil.NOTAUTHZ_EXCEPTION;
      client2.invoke(SecurityTestUtil.class, "doMultiUserGets", new Object[] {
          Integer.valueOf(1), Integer.valueOf(1),
          new Integer[] {expectedResult}});
    }
  }

  private void verifyContainsKeyDestroys() throws Exception {
    verifyContainsKeyDestroys(true, false /* unused */);
  }

  private void verifyContainsKeyDestroys(Boolean isMultiuser, Boolean opAllowed)
      throws Exception {
    // Do puts before verifying containsKey
    client1.invoke(SecurityTestUtil.class, "doMultiUserPuts", new Object[] {
        Integer.valueOf(2),
        Integer.valueOf(2),
        new Integer[] {SecurityTestUtil.NO_EXCEPTION,
            SecurityTestUtil.NO_EXCEPTION}});
    client1.invoke(SecurityTestUtil.class, "doMultiUserContainsKeys",
        new Object[] {
            Integer.valueOf(1),
            Integer.valueOf(2),
            new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                SecurityTestUtil.NOTAUTHZ_EXCEPTION},
            new Boolean[] {Boolean.TRUE, Boolean.FALSE}});

    // Verify that the destroys succeed/fail
    if (isMultiuser) {
      client2.invoke(SecurityTestUtil.class, "doMultiUserDestroys",
          new Object[] {
              Integer.valueOf(2),
              Integer.valueOf(2),
              new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                  SecurityTestUtil.NOTAUTHZ_EXCEPTION}});
    } else {
      int expectedResult = (opAllowed) ? SecurityTestUtil.NO_EXCEPTION
          : SecurityTestUtil.NOTAUTHZ_EXCEPTION;
      client2.invoke(SecurityTestUtil.class, "doMultiUserDestroys",
          new Object[] {Integer.valueOf(1), Integer.valueOf(1),
              new Integer[] {expectedResult}});
    }
  }

  private void verifyContainsKeyInvalidates() throws Exception {
    verifyContainsKeyInvalidates(true, false /* unused */);
  }

  private void verifyContainsKeyInvalidates(Boolean isMultiuser, Boolean opAllowed)
      throws Exception {
    // Do puts before verifying containsKey
    client1.invoke(SecurityTestUtil.class, "doMultiUserPuts", new Object[] {
        Integer.valueOf(2),
        Integer.valueOf(2),
        new Integer[] {SecurityTestUtil.NO_EXCEPTION,
            SecurityTestUtil.NO_EXCEPTION}});
    client1.invoke(SecurityTestUtil.class, "doMultiUserContainsKeys",
        new Object[] {
            Integer.valueOf(1),
            Integer.valueOf(2),
            new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                SecurityTestUtil.NOTAUTHZ_EXCEPTION},
            new Boolean[] {Boolean.TRUE, Boolean.FALSE}});

    // Verify that the invalidates succeed/fail
    if (isMultiuser) {
      client2.invoke(SecurityTestUtil.class, "doMultiUserInvalidates",
          new Object[] {
              Integer.valueOf(2),
              Integer.valueOf(2),
              new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                  SecurityTestUtil.NOTAUTHZ_EXCEPTION}});
    } else {
      int expectedResult = (opAllowed) ? SecurityTestUtil.NO_EXCEPTION
          : SecurityTestUtil.NOTAUTHZ_EXCEPTION;
      client2.invoke(SecurityTestUtil.class, "doMultiUserInvalidates",
          new Object[] {Integer.valueOf(1), Integer.valueOf(1),
              new Integer[] {expectedResult}});
    }
  }

  private void verifyGetAllInTX() {
    server1.invoke(ClientMultiUserAuthzDUnitTest.class, "doPuts");
    client1.invoke(SecurityTestUtil.class, "doMultiUserGetAll", new Object[] {
      Integer.valueOf(2),
      new Integer[] {SecurityTestUtil.NO_EXCEPTION,
          SecurityTestUtil.NOTAUTHZ_EXCEPTION}, Boolean.TRUE/*use TX*/});
  }

  private void verifyGetAllRegionDestroys() {
    server1.invoke(ClientMultiUserAuthzDUnitTest.class, "doPuts");
    client1.invoke(SecurityTestUtil.class, "doMultiUserGetAll", new Object[] {
      Integer.valueOf(2),
      new Integer[] {SecurityTestUtil.NO_EXCEPTION,
          SecurityTestUtil.NOTAUTHZ_EXCEPTION}});

    // Verify that the region destroys succeed/fail
    client2.invoke(SecurityTestUtil.class, "doMultiUserRegionDestroys",
        new Object[] {
            Integer.valueOf(2),
            new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                SecurityTestUtil.NOTAUTHZ_EXCEPTION}});
  }
  
  public static void doPuts() {
    Region region = GemFireCacheImpl.getInstance().getRegion(SecurityTestUtil.regionName);
    region.put("key1", "value1");
    region.put("key2", "value2");
  }

  // Test query/function execute
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
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Integer port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      Integer port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      server1.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] {SecurityTestUtil.getLocatorPort(), port1, serverProps,
              javaProps});
      server2.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] {SecurityTestUtil.getLocatorPort(), port2, serverProps,
              javaProps});

      // Start client1 with valid/invalid QUERY credentials
      Properties[] client1Credentials = new Properties[] {
          gen.getAllowedCredentials(
                  new OperationCode[] {OperationCode.PUT, OperationCode.QUERY},
                  new String[] {regionName},
                  1),
          gen.getDisallowedCredentials(
                  new OperationCode[] {OperationCode.PUT, OperationCode.QUERY},
                  new String[] {regionName},
                  1)
      };

      javaProps = cGen.getJavaProperties();
      getLogWriter().info(
          "testOps2: For first client credentials: " + client1Credentials[0]
              + "\n" + client1Credentials[1]);
      client1.invoke(SecurityTestUtil.class,
          "createCacheClientForMultiUserMode", new Object[] {
              Integer.valueOf(2), authInit, client1Credentials, javaProps,
              new Integer[] {port1, port2}, null, Boolean.FALSE,
              SecurityTestUtil.NO_EXCEPTION});

      // Start client2 with valid/invalid EXECUTE_FUNCTION credentials
      Properties[] client2Credentials = new Properties[] {
          gen.getAllowedCredentials(new OperationCode[] {OperationCode.EXECUTE_FUNCTION},
              new String[] {regionName}, 2),
          gen.getDisallowedCredentials(new OperationCode[] {OperationCode.EXECUTE_FUNCTION},
              new String[] {regionName}, 9)};

      javaProps = cGen.getJavaProperties();
      getLogWriter().info(
          "testOps2: For second client credentials: " + client2Credentials[0]
              + "\n" + client2Credentials[1]);
      client2.invoke(SecurityTestUtil.class,
          "createCacheClientForMultiUserMode", new Object[] {
              Integer.valueOf(2), authInit, client2Credentials, javaProps,
              new Integer[] {port1, port2}, null, Boolean.FALSE,
              SecurityTestUtil.NO_EXCEPTION});
      Function function = new TestFunction(true,TestFunction.TEST_FUNCTION1);
      server1.invoke(PRClientServerTestBase.class,
          "registerFunction", new Object []{function});
      
      server2.invoke(PRClientServerTestBase.class,
          "registerFunction", new Object []{function});
      
      // Perform some put operations before verifying queries
      client1.invoke(SecurityTestUtil.class, "doMultiUserPuts", new Object[] {
          Integer.valueOf(4),
          Integer.valueOf(2),
          new Integer[] {SecurityTestUtil.NO_EXCEPTION,
              SecurityTestUtil.NOTAUTHZ_EXCEPTION}});
      client1.invoke(SecurityTestUtil.class, "doMultiUserQueries",
          new Object[] {
              Integer.valueOf(2),
              new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                  SecurityTestUtil.NOTAUTHZ_EXCEPTION}, Integer.valueOf(4)});
      client1.invoke(SecurityTestUtil.class, "doMultiUserQueryExecute",
          new Object[] {
              Integer.valueOf(2),
              new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                  SecurityTestUtil.NOTAUTHZ_EXCEPTION}, Integer.valueOf(4)});

      // Verify that the FE succeeds/fails
      client2.invoke(SecurityTestUtil.class, "doMultiUserFE", new Object[] {
          Integer.valueOf(2),
          function,
          new Integer[] {SecurityTestUtil.NO_EXCEPTION,
              SecurityTestUtil.NOTAUTHZ_EXCEPTION}, new Object[] {null, null},
          Boolean.FALSE});

      // Failover
      server1.invoke(SecurityTestUtil.class, "closeCache");
      Thread.sleep(2000);

      client1.invoke(SecurityTestUtil.class, "doMultiUserPuts", new Object[] {
          Integer.valueOf(4),
          Integer.valueOf(2),
          new Integer[] {SecurityTestUtil.NO_EXCEPTION,
              SecurityTestUtil.NOTAUTHZ_EXCEPTION}});

      client1.invoke(SecurityTestUtil.class, "doMultiUserQueries",
          new Object[] {
              Integer.valueOf(2),
              new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                  SecurityTestUtil.NOTAUTHZ_EXCEPTION}, Integer.valueOf(4)});
      client1.invoke(SecurityTestUtil.class, "doMultiUserQueryExecute",
          new Object[] {
              Integer.valueOf(2),
              new Integer[] {SecurityTestUtil.NO_EXCEPTION,
                  SecurityTestUtil.NOTAUTHZ_EXCEPTION}, Integer.valueOf(4)});

      // Verify that the FE succeeds/fails
      client2.invoke(SecurityTestUtil.class, "doMultiUserFE", new Object[] {
          Integer.valueOf(2),
          function,
          new Integer[] {SecurityTestUtil.NO_EXCEPTION,
              SecurityTestUtil.NOTAUTHZ_EXCEPTION}, new Object[] {null, null},
          Boolean.TRUE});


  }

  public void testOpsWithClientsInDifferentModes() throws Exception {
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

      getLogWriter().info("testOpsWithClientsInDifferentModes: Using authinit: " + authInit);
      getLogWriter().info(
          "testOpsWithClientsInDifferentModes: Using authenticator: " + authenticator);
      getLogWriter().info("testOpsWithClientsInDifferentModes: Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Integer port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      Integer port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      server1.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] {SecurityTestUtil.getLocatorPort(), port1, serverProps,
              javaProps});
      server2.invoke(ClientAuthorizationTestBase.class, "createCacheServer",
          new Object[] {SecurityTestUtil.getLocatorPort(), port2, serverProps,
              javaProps});

      if (!prepareClientsForOps(gen, cGen, new OperationCode[] {
          OperationCode.PUT, OperationCode.PUT}, new OperationCode[] {
          OperationCode.GET, OperationCode.GET}, javaProps, authInit, port1,
          port2, Boolean.FALSE, Boolean.TRUE)) {
        continue;
      }
      verifyPutsGets(false, true);

      if (!prepareClientsForOps(gen, cGen, new OperationCode[] {
          OperationCode.PUT, OperationCode.CONTAINS_KEY}, new OperationCode[] {
          OperationCode.DESTROY, OperationCode.DESTROY},
          javaProps, authInit, port1, port2, Boolean.FALSE, Boolean.FALSE)) {
        continue;
      }
      verifyContainsKeyDestroys(false, false);
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
