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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.query.internal.cq.ClientCQImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * @author ashetkar
 *
 */
public class MultiuserDurableCQAuthzDUnitTest extends
    ClientAuthorizationTestBase {
  
  public static final Map<String, String> cqNameToQueryStrings = new HashMap<String, String>();

  static {
    cqNameToQueryStrings.put("CQ_0", "SELECT * FROM ");
    cqNameToQueryStrings.put("CQ_1", "SELECT * FROM ");
  }

  public MultiuserDurableCQAuthzDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });

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

  public void testCQForDurableClientsWithDefaultClose() throws Exception {
    /*
     *  1. Start a server.
     *  2. Start a durable client in mulituser secure mode.
     *  3. Create two users registering unique durable CQs on server.
     *  4. Invoke GemFireCache.close() at client.
     *  5. Put some events on server satisfying both the CQs.
     *  6. Up the client and the two users.
     *  7. Confirm that the users receive the events which were enqueued at server while they were away.
     *  8. Same for ProxyCache.close()
     */
    Integer numOfUsers = 2;
    Integer numOfPuts = 5;
    Boolean[] postAuthzAllowed = new Boolean[] {Boolean.TRUE, Boolean.TRUE};

    doTest(numOfUsers, numOfPuts, postAuthzAllowed,
          getXmlAuthzGenerator(), null);
  }

  public void testCQForDurableClientsWithCloseKeepAliveTrue() throws Exception {
    /*
     *  1. Start a server.
     *  2. Start a durable client in mulituser secure mode.
     *  3. Create two users registering unique durable CQs on server.
     *  4. Invoke GemFireCache.close(false) at client.
     *  5. Put some events on server satisfying both the CQs.
     *  6. Up the client and the two users.
     *  7. Observer the behaviour.
     *  8. Same for ProxyCache.close(false)
     */
    Integer numOfUsers = 2;
    Integer numOfPuts = 5;
    Boolean[] postAuthzAllowed = new Boolean[] {Boolean.TRUE, Boolean.TRUE};

    doTest(numOfUsers, numOfPuts, postAuthzAllowed,
          getXmlAuthzGenerator(), Boolean.TRUE);
  }

  public void testCQForDurableClientsWithCloseKeepAliveFalse() throws Exception {
    /*
     *  1. Start a server.
     *  2. Start a durable client in mulituser secure mode.
     *  3. Create two users registering unique durable CQs on server.
     *  4. Invoke GemFireCache.close(true) at client.
     *  5. Put some events on server satisfying both the CQs.
     *  6. Up the client and the two users.
     *  7. Observer the behaviour.
     *  8. Same for ProxyCache.close(true)
     */
    Integer numOfUsers = 2;
    Integer numOfPuts = 5;
    Boolean[] postAuthzAllowed = new Boolean[] {Boolean.TRUE, Boolean.TRUE};

    doTest(numOfUsers, numOfPuts, postAuthzAllowed,
              getXmlAuthzGenerator(), Boolean.FALSE);
  }

  private void doTest(Integer numOfUsers, Integer numOfPuts,
      Boolean[] postAuthzAllowed, AuthzCredentialGenerator gen, Boolean keepAlive)
      throws Exception {
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String accessor = gen.getAuthorizationCallback();
    String authInit = cGen.getAuthInit();
    TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

    Properties serverProps = buildProperties(authenticator, accessor, true,
        extraAuthProps, extraAuthzProps);

    Properties opCredentials;
    cGen = tgen.getCredentialGenerator();
    Properties javaProps2 = null;
    if (cGen != null) {
      javaProps2 = cGen.getJavaProperties();
    }

    int[] indices = new int[numOfPuts];
    for (int index = 0; index < numOfPuts; ++index) {
      indices[index] = index;
    }

    Random rnd = new Random();
    Properties[] authProps = new Properties[numOfUsers];
    String durableClientId = "multiuser_durable_client_1";
    Properties client2Credentials = null;
    for (int i = 0; i < numOfUsers; i++) {
      int rand = rnd.nextInt(100) + 1;
      if (postAuthzAllowed[i]) {
        opCredentials = tgen.getAllowedCredentials(new OperationCode[] {
            OperationCode.EXECUTE_CQ, OperationCode.GET}, // For callback, GET should be allowed
            new String[] {regionName}, indices, rand);
      } else {
        opCredentials = tgen.getDisallowedCredentials(new OperationCode[] {
            OperationCode.GET}, // For callback, GET should be disallowed
            new String[] {regionName}, indices, rand);
      }
      authProps[i] = SecurityTestUtil.concatProperties(new Properties[] {
          opCredentials, extraAuthProps, extraAuthzProps});

      if (client2Credentials == null) {
        client2Credentials = tgen.getAllowedCredentials(new OperationCode[] {
            OperationCode.PUT},
            new String[] {regionName}, indices, rand);
      }
    }

    // Get ports for the servers
    Integer port1 = new Integer(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));
    Integer port2 = new Integer(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));
    Integer locatorPort = new Integer(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));
    // Close down any running servers
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");

    server1.invoke(MultiuserDurableCQAuthzDUnitTest.class,
        "createServerCache", new Object[] {serverProps, javaProps, locatorPort, port1});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class,
        "createClientCache", new Object[] {javaProps2, authInit, authProps,
            new Integer[] {port1, port2}, numOfUsers, durableClientId, postAuthzAllowed});

//    client2.invoke(SecurityTestUtil.class, "createCacheClient",
//        new Object[] {authInit, client2Credentials, javaProps2,
//            new Integer[] {port1, port2}, null, SecurityTestUtil.NO_EXCEPTION});

    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "createCQ",
        new Object[] {numOfUsers, Boolean.TRUE});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "executeCQ",
        new Object[] {numOfUsers, new Boolean[] {false, false}, numOfPuts,
            new String[numOfUsers]});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "readyForEvents");

    if (keepAlive == null) {
      client1.invoke(SecurityTestUtil.class, "closeCache");
    } else {
      client1.invoke(SecurityTestUtil.class, "closeCache",
          new Object[] {keepAlive});
    }

    server1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "doPuts",
        new Object[] {numOfPuts, Boolean.TRUE/* put last key */});

    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class,
        "createClientCache", new Object[] {javaProps2, authInit, authProps,
            new Integer[] {port1, port2}, numOfUsers, durableClientId, postAuthzAllowed});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "createCQ",
        new Object[] {numOfUsers, Boolean.TRUE});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "executeCQ",
        new Object[] {numOfUsers, new Boolean[] {false, false}, numOfPuts,
            new String[numOfUsers]});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "readyForEvents");

    if (!postAuthzAllowed[0] || keepAlive == null || !keepAlive) {
      // Don't wait as no user is authorized to receive cq events.
      Thread.sleep(1000);
    } else {
      client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "waitForLastKey",
          new Object[] {Integer.valueOf(0), Boolean.TRUE});
    }
    Integer numOfCreates = (keepAlive == null) ? 0
        : (keepAlive) ? (numOfPuts + 1/* last key */) : 0;
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "checkCQListeners",
        new Object[] {numOfUsers, postAuthzAllowed, numOfCreates, 0});

    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "proxyCacheClose",
        new Object[] {new Integer[] {0, 1}, keepAlive});

    client1.invoke(SecurityTestUtil.class, "createProxyCache",
        new Object[] {new Integer[] {0, 1}, authProps});

    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "createCQ",
        new Object[] {numOfUsers, Boolean.TRUE});
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "executeCQ",
        new Object[] {numOfUsers, new Boolean[] {false, false}, numOfPuts,
            new String[numOfUsers]});

    server1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "doPuts",
        new Object[] {numOfPuts, Boolean.TRUE/* put last key */});

    if (!postAuthzAllowed[0] || keepAlive == null || !keepAlive) {
      // Don't wait as no user is authorized to receive cq events.
      Thread.sleep(1000);
    } else {
      client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "waitForLastKey",
          new Object[] {Integer.valueOf(0), Boolean.FALSE});
    }
    Integer numOfUpdates = numOfPuts + 1;
    client1.invoke(MultiuserDurableCQAuthzDUnitTest.class, "checkCQListeners",
        new Object[] {numOfUsers, postAuthzAllowed, 0, numOfUpdates});
  }

  public static void createServerCache(Properties serverProps,
      Properties javaProps, Integer locatorPort, Integer serverPort) {
    SecurityTestUtil.createCacheServer((Properties)serverProps, javaProps,
        locatorPort, null, serverPort, Boolean.TRUE, new Integer(
            SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createClientCache(Properties javaProps,
      String authInit, Properties[] authProps, Integer ports[],
      Integer numOfUsers, Boolean[] postAuthzAllowed) {
    SecurityTestUtil.createCacheClientForMultiUserMode(numOfUsers, authInit,
        authProps, javaProps, ports, null, Boolean.FALSE,
        SecurityTestUtil.NO_EXCEPTION);
  }

  public static void readyForEvents() {
    GemFireCacheImpl.getInstance().readyForEvents();
  }

  public static void createClientCache(Properties javaProps,
      String authInit, Properties[] authProps, Integer ports[],
      Integer numOfUsers, String durableId, Boolean[] postAuthzAllowed) {
    SecurityTestUtil.createCacheClientForMultiUserMode(numOfUsers, authInit,
        authProps, javaProps, ports, null, Boolean.FALSE, durableId,
        SecurityTestUtil.NO_EXCEPTION);
  }

  public static void createCQ(Integer num) {
    createCQ(num, false);
  }

  public static void createCQ(Integer num, Boolean isDurable) {
    for (int i = 0; i < num; i++) {
      QueryService cqService = SecurityTestUtil.proxyCaches[i].getQueryService();
      String cqName = "CQ_" + i;
      String queryStr = cqNameToQueryStrings.get(cqName)
          + SecurityTestUtil.proxyCaches[i].getRegion(regionName).getFullPath();
      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
      ((CqQueryTestListener)cqListeners[0]).cqName = cqName;

      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      // Create CQ.
      try {
        CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa, isDurable);
        assertTrue("newCq() state mismatch", cq1.getState().isStopped());
      } catch (Exception ex) {
        AssertionError err = new AssertionError("Failed to create CQ " + cqName
            + " . ");
        err.initCause(ex);
        getLogWriter().info("CqService is :" + cqService, err);
        throw err;
      }
    }
  }

  public static void executeCQ(Integer num, Boolean[] initialResults,
      Integer expectedResultsSize, String[] expectedErr) {
    InternalLogWriter logWriter = InternalDistributedSystem.getStaticInternalLogWriter();
    for (int i = 0; i < num; i++) {
      try {
        if (expectedErr[i] != null) {
          logWriter.info(
              "<ExpectedException action=add>" + expectedErr[i]
                  + "</ExpectedException>");
        }
        CqQuery cq1 = null;
        String cqName = "CQ_" + i;
        String queryStr = cqNameToQueryStrings.get(cqName)
            + SecurityTestUtil.proxyCaches[i].getRegion(regionName)
                .getFullPath();
        QueryService cqService = SecurityTestUtil.proxyCaches[i]
            .getQueryService();

        // Get CqQuery object.
        try {
          cq1 = cqService.getCq(cqName);
          if (cq1 == null) {
            getLogWriter().info(
                "Failed to get CqQuery object for CQ name: " + cqName);
            fail("Failed to get CQ " + cqName);
          } else {
            getLogWriter().info("Obtained CQ, CQ name: " + cq1.getName());
            assertTrue("newCq() state mismatch", cq1.getState().isStopped());
          }
        } catch (Exception ex) {
          getLogWriter().info("CqService is :" + cqService);
          getLogWriter().error(ex);
          AssertionError err = new AssertionError("Failed to execute CQ "
              + cqName);
          err.initCause(ex);
          throw err;
        }

        if (initialResults[i]) {
          SelectResults cqResults = null;

          try {
            cqResults = cq1.executeWithInitialResults();
          } catch (Exception ex) {
            getLogWriter().info("CqService is: " + cqService);
            ex.printStackTrace();
            AssertionError err = new AssertionError("Failed to execute CQ "
                + cqName);
            err.initCause(ex);
            throw err;
          }
          getLogWriter().info("initial result size = " + cqResults.size());
          assertTrue("executeWithInitialResults() state mismatch", cq1
              .getState().isRunning());
          if (expectedResultsSize >= 0) {
            assertEquals("unexpected results size", expectedResultsSize
                .intValue(), cqResults.size());
          }
        } else {
          try {
            cq1.execute();
          } catch (Exception ex) {
            AssertionError err = new AssertionError("Failed to execute CQ "
                + cqName);
            err.initCause(ex);
            if (expectedErr == null) {
              getLogWriter().info("CqService is: " + cqService, err);
            }
            throw err;
          }
          assertTrue("execute() state mismatch", cq1.getState().isRunning());
        }
      } finally {
        if (expectedErr[i] != null) {
          logWriter.info(
              "<ExpectedException action=remove>" + expectedErr[i]
                  + "</ExpectedException>");
        }
      }
    }
  }

  public static void doPuts(Integer num, Boolean putLastKey) {
    Region region = GemFireCacheImpl.getInstance().getRegion(regionName);
    for (int i = 0; i < num; i++) {
      region.put("CQ_key"+i, "CQ_value"+i);
    }
    if (putLastKey) {
      region.put("LAST_KEY", "LAST_KEY");
    }
  }

  public static void putLastKey() {
    Region region = GemFireCacheImpl.getInstance().getRegion(regionName);
    region.put("LAST_KEY", "LAST_KEY");
  }

  public static void waitForLastKey(Integer cqIndex, Boolean isCreate) {
    String cqName = "CQ_" + cqIndex;
    QueryService qService = SecurityTestUtil.proxyCaches[cqIndex].getQueryService();
    ClientCQImpl cqQuery = (ClientCQImpl)qService.getCq(cqName);
    if (isCreate) {
      ((CqQueryTestListener)cqQuery.getCqListeners()[cqIndex])
          .waitForCreated("LAST_KEY");
    } else {
      ((CqQueryTestListener)cqQuery.getCqListeners()[cqIndex])
          .waitForUpdated("LAST_KEY");
    }
  }

  public static void checkCQListeners(Integer numOfUsers,
      Boolean[] expectedListenerInvocation, Integer createEventsSize,
      Integer updateEventsSize) {
    for (int i = 0; i < numOfUsers; i++) {
      String cqName = "CQ_" + i;
      QueryService qService = SecurityTestUtil.proxyCaches[i].getQueryService();
      ClientCQImpl cqQuery = (ClientCQImpl)qService.getCq(cqName);
      if (expectedListenerInvocation[i]) {
        for (CqListener listener : cqQuery.getCqListeners()) {
          assertEquals(createEventsSize.intValue(),
              ((CqQueryTestListener)listener).getCreateEventCount());
          assertEquals(updateEventsSize.intValue(),
              ((CqQueryTestListener)listener).getUpdateEventCount());
        }
      } else {
        for (CqListener listener : cqQuery.getCqListeners()) {
          assertEquals(0, ((CqQueryTestListener)listener).getTotalEventCount());
        }
      }
    }
  }

  public static void proxyCacheClose(Integer[] userIndices) {
    proxyCacheClose(userIndices, null);
  }

  public static void proxyCacheClose(Integer[] userIndices, Boolean keepAliveFlags) {
    if (keepAliveFlags != null) {
      for (int i : userIndices) {
        SecurityTestUtil.proxyCaches[i].close(keepAliveFlags);
      }
    } else {
      for (int i : userIndices) {
        SecurityTestUtil.proxyCaches[i].close();
      }
    }
  }

}
