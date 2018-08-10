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

import static org.apache.geode.security.SecurityTestUtils.NO_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.security.SecurityTestUtils.concatProperties;
import static org.apache.geode.security.SecurityTestUtils.createCacheClientForMultiUserMode;
import static org.apache.geode.security.SecurityTestUtils.createProxyCache;
import static org.apache.geode.security.SecurityTestUtils.getProxyCaches;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.cq.dunit.CqQueryTestListener;
import org.apache.geode.cache.query.internal.cq.ClientCQImpl;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class MultiUserDurableCQAuthzDUnitTest extends ClientAuthorizationTestCase {

  private final Map<String, String> cqNameToQueryStrings = new HashMap<>();

  @Override
  public final void preSetUpClientAuthorizationTestBase() throws Exception {
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
  }

  @Override
  public final void postSetUpClientAuthorizationTestBase() throws Exception {
    cqNameToQueryStrings.put("CQ_0", "SELECT * FROM ");
    cqNameToQueryStrings.put("CQ_1", "SELECT * FROM ");
  }

  @Override
  public final void postTearDownClientAuthorizationTestBase() throws Exception {
    cqNameToQueryStrings.clear();
  }

  @Test
  public void testCQForDurableClientsWithDefaultClose() throws Exception {
    /*
     * 1. Start a server. 2. Start a durable client in mulituser secure mode. 3. Create two users
     * registering unique durable CQs on server. 4. Invoke GemFireCache.close() at client. 5. Put
     * some events on server satisfying both the CQs. 6. Up the client and the two users. 7. Confirm
     * that the users receive the events which were enqueued at server while they were away. 8. Same
     * for ProxyCache.close()
     */
    int numOfUsers = 2;
    int numOfPuts = 5;
    boolean[] postAuthzAllowed = new boolean[] {true, true};

    doTest(numOfUsers, numOfPuts, postAuthzAllowed, getXmlAuthzGenerator(), null);
  }

  @Test
  public void testCQForDurableClientsWithCloseKeepAliveTrue() throws Exception {
    /*
     * 1. Start a server. 2. Start a durable client in mulituser secure mode. 3. Create two users
     * registering unique durable CQs on server. 4. Invoke GemFireCache.close(false) at client. 5.
     * Put some events on server satisfying both the CQs. 6. Up the client and the two users. 7.
     * Observer the behaviour. 8. Same for ProxyCache.close(false)
     */
    int numOfUsers = 2;
    int numOfPuts = 5;
    boolean[] postAuthzAllowed = new boolean[] {true, true};

    doTest(numOfUsers, numOfPuts, postAuthzAllowed, getXmlAuthzGenerator(), true);
  }

  @Test
  public void testCQForDurableClientsWithCloseKeepAliveFalse() throws Exception {
    /*
     * 1. Start a server. 2. Start a durable client in mulituser secure mode. 3. Create two users
     * registering unique durable CQs on server. 4. Invoke GemFireCache.close(true) at client. 5.
     * Put some events on server satisfying both the CQs. 6. Up the client and the two users. 7.
     * Observer the behaviour. 8. Same for ProxyCache.close(true)
     */
    int numOfUsers = 2;
    int numOfPuts = 5;
    boolean[] postAuthzAllowed = new boolean[] {true, true};

    doTest(numOfUsers, numOfPuts, postAuthzAllowed, getXmlAuthzGenerator(), false);
  }

  /**
   * WARNING: "final Boolean keepAlive" is treated as a ternary value: null, true, false
   */
  private void doTest(int numOfUsers, int numOfPuts, boolean[] postAuthzAllowed,
      final AuthzCredentialGenerator authzGenerator, final Boolean keepAlive) throws Exception {
    CredentialGenerator credentialGenerator = authzGenerator.getCredentialGenerator();
    Properties extraAuthProps = credentialGenerator.getSystemProperties();
    Properties javaProps = credentialGenerator.getJavaProperties();
    Properties extraAuthzProps = authzGenerator.getSystemProperties();
    String authenticator = credentialGenerator.getAuthenticator();
    String accessor = authzGenerator.getAuthorizationCallback();
    String authInit = credentialGenerator.getAuthInit();
    TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(authzGenerator);

    Properties serverProps =
        buildProperties(authenticator, accessor, true, extraAuthProps, extraAuthzProps);

    Properties opCredentials;
    credentialGenerator = tgen.getCredentialGenerator();
    final Properties javaProps2 =
        credentialGenerator != null ? credentialGenerator.getJavaProperties() : null;

    int[] indices = new int[numOfPuts];
    for (int index = 0; index < numOfPuts; ++index) {
      indices[index] = index;
    }

    Random random = new Random();
    Properties[] authProps = new Properties[numOfUsers];
    String durableClientId = "multiuser_durable_client_1";

    Properties client2Credentials = null;

    for (int i = 0; i < numOfUsers; i++) {
      int rand = random.nextInt(100) + 1;
      if (postAuthzAllowed[i]) {
        opCredentials = tgen.getAllowedCredentials(
            new OperationCode[] {OperationCode.EXECUTE_CQ, OperationCode.GET}, // For callback, GET
                                                                               // should be allowed
            new String[] {regionName}, indices, rand);

      } else {
        opCredentials = tgen.getDisallowedCredentials(new OperationCode[] {OperationCode.GET}, // For
                                                                                               // callback,
                                                                                               // GET
                                                                                               // should
                                                                                               // be
                                                                                               // disallowed
            new String[] {regionName}, indices, rand);
      }

      authProps[i] =
          concatProperties(new Properties[] {opCredentials, extraAuthProps, extraAuthzProps});

      if (client2Credentials == null) {
        client2Credentials = tgen.getAllowedCredentials(new OperationCode[] {OperationCode.PUT},
            new String[] {regionName}, indices, rand);
      }
    }

    // Get ports for the servers
    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = randomAvailableTCPPorts[0];
    int port2 = randomAvailableTCPPorts[1];

    // Close down any running servers
    server1.invoke(() -> closeCache());
    server2.invoke(() -> closeCache());

    server1.invoke(() -> createServerCache(serverProps, javaProps, port1));
    client1.invoke(() -> createClientCache(javaProps2, authInit, authProps,
        new int[] {port1, port2}, numOfUsers, durableClientId, postAuthzAllowed));

    client1.invoke(() -> createCQ(numOfUsers, true));
    client1.invoke(() -> executeCQ(numOfUsers, new boolean[] {false, false}, numOfPuts,
        new String[numOfUsers]));
    client1.invoke(() -> readyForEvents());

    if (keepAlive == null) {
      client1.invoke(() -> closeCache());
    } else {
      client1.invoke(() -> closeCache(keepAlive));
    }

    server1.invoke(() -> doPuts(numOfPuts, true/* put last key */));

    client1.invoke(() -> createClientCache(javaProps2, authInit, authProps,
        new int[] {port1, port2}, numOfUsers, durableClientId, postAuthzAllowed));
    client1.invoke(() -> createCQ(numOfUsers, true));
    client1.invoke(() -> executeCQ(numOfUsers, new boolean[] {false, false}, numOfPuts,
        new String[numOfUsers]));
    client1.invoke(() -> readyForEvents());

    if (!postAuthzAllowed[0] || keepAlive == null || !keepAlive) {
      // Don't wait as no user is authorized to receive cq events.
      Thread.sleep(1000); // TODO: use Awaitility
    } else {
      client1.invoke(() -> waitForLastKey(0, true));
    }

    int numOfCreates = keepAlive == null ? 0 : (keepAlive ? numOfPuts + 1/* last key */ : 0);
    client1.invoke(() -> checkCQListeners(numOfUsers, postAuthzAllowed, numOfCreates, 0));
    client1.invoke(() -> proxyCacheClose(new int[] {0, 1}, keepAlive));
    client1.invoke(() -> createProxyCache(new int[] {0, 1}, authProps));
    client1.invoke(() -> createCQ(numOfUsers, true));
    client1.invoke(() -> executeCQ(numOfUsers, new boolean[] {false, false}, numOfPuts,
        new String[numOfUsers]));

    server1.invoke(() -> doPuts(numOfPuts, true/* put last key */));

    if (!postAuthzAllowed[0] || keepAlive == null || !keepAlive) {
      // Don't wait as no user is authorized to receive cq events.
      Thread.sleep(1000); // TODO: use Awaitility
    } else {
      client1.invoke(() -> waitForLastKey(0, false));
    }

    int numOfUpdates = numOfPuts + 1;
    client1.invoke(() -> checkCQListeners(numOfUsers, postAuthzAllowed, 0, numOfUpdates));
  }

  private void createServerCache(final Properties serverProps, final Properties javaProps,
      final int serverPort) {
    SecurityTestUtils.createCacheServer(serverProps, javaProps, serverPort, true,
        NO_EXCEPTION);
  }

  private void readyForEvents() {
    GemFireCacheImpl.getInstance().readyForEvents();
  }

  /**
   * NOTE: "final boolean[] postAuthzAllowed" is never used
   */
  private void createClientCache(final Properties javaProps, final String authInit,
      final Properties[] authProps, final int ports[], final int numOfUsers, final String durableId,
      final boolean[] postAuthzAllowed) {
    createCacheClientForMultiUserMode(numOfUsers, authInit, authProps, javaProps, ports, 0, false,
        durableId, NO_EXCEPTION);
  }

  private void createCQ(final int num, final boolean isDurable)
      throws CqException, CqExistsException {
    for (int i = 0; i < num; i++) {
      QueryService cqService = getProxyCaches(i).getQueryService();
      String cqName = "CQ_" + i;
      String queryStr =
          cqNameToQueryStrings.get(cqName) + getProxyCaches(i).getRegion(regionName).getFullPath();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
      ((CqQueryTestListener) cqListeners[0]).cqName = cqName;

      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      // Create CQ.
      CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa, isDurable);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
    }
  }

  private void executeCQ(final int num, final boolean[] initialResults,
      final int expectedResultsSize, final String[] expectedErr)
      throws CqException, RegionNotFoundException {
    for (int i = 0; i < num; i++) {
      try {
        if (expectedErr[i] != null) {
          getLogWriter()
              .info("<ExpectedException action=add>" + expectedErr[i] + "</ExpectedException>");
        }

        CqQuery cq1 = null;
        String cqName = "CQ_" + i;
        String queryStr = cqNameToQueryStrings.get(cqName)
            + getProxyCaches(i).getRegion(regionName).getFullPath();
        QueryService cqService = getProxyCaches(i).getQueryService();

        // Get CqQuery object.
        cq1 = cqService.getCq(cqName);
        if (cq1 == null) {
          getLogWriter().info("Failed to get CqQuery object for CQ name: " + cqName);
          fail("Failed to get CQ " + cqName);

        } else {
          getLogWriter().info("Obtained CQ, CQ name: " + cq1.getName());
          assertTrue("newCq() state mismatch", cq1.getState().isStopped());
        }

        if (initialResults[i]) {
          SelectResults cqResults = null;

          cqResults = cq1.executeWithInitialResults();

          getLogWriter().info("initial result size = " + cqResults.size());
          assertTrue("executeWithInitialResults() state mismatch", cq1.getState().isRunning());
          if (expectedResultsSize >= 0) {
            assertEquals("unexpected results size", expectedResultsSize, cqResults.size());
          }

        } else {
          cq1.execute();
          assertTrue("execute() state mismatch", cq1.getState().isRunning());
        }

      } finally {
        if (expectedErr[i] != null) {
          getLogWriter()
              .info("<ExpectedException action=remove>" + expectedErr[i] + "</ExpectedException>");
        }
      }
    }
  }

  private void doPuts(final int num, final boolean putLastKey) {
    Region region = GemFireCacheImpl.getInstance().getRegion(regionName);
    for (int i = 0; i < num; i++) {
      region.put("CQ_key" + i, "CQ_value" + i);
    }
    if (putLastKey) {
      region.put("LAST_KEY", "LAST_KEY");
    }
  }

  private void waitForLastKey(final int cqIndex, final boolean isCreate) {
    String cqName = "CQ_" + cqIndex;
    QueryService qService = getProxyCaches(cqIndex).getQueryService();
    ClientCQImpl cqQuery = (ClientCQImpl) qService.getCq(cqName);
    if (isCreate) {
      ((CqQueryTestListener) cqQuery.getCqListeners()[cqIndex]).waitForCreated("LAST_KEY");
    } else {
      ((CqQueryTestListener) cqQuery.getCqListeners()[cqIndex]).waitForUpdated("LAST_KEY");
    }
  }

  private void checkCQListeners(final int numOfUsers, final boolean[] expectedListenerInvocation,
      final int createEventsSize, final int updateEventsSize) {
    for (int i = 0; i < numOfUsers; i++) {
      String cqName = "CQ_" + i;
      QueryService qService = getProxyCaches(i).getQueryService();
      ClientCQImpl cqQuery = (ClientCQImpl) qService.getCq(cqName);

      if (expectedListenerInvocation[i]) {
        for (CqListener listener : cqQuery.getCqListeners()) {
          assertEquals(createEventsSize, ((CqQueryTestListener) listener).getCreateEventCount());
          assertEquals(updateEventsSize, ((CqQueryTestListener) listener).getUpdateEventCount());
        }

      } else {
        for (CqListener listener : cqQuery.getCqListeners()) {
          assertEquals(0, ((CqQueryTestListener) listener).getTotalEventCount());
        }
      }
    }
  }

  /**
   * WARNING: "final Boolean keepAliveFlags" is treated as a ternary: null, true, false
   */
  private void proxyCacheClose(final int[] userIndices, final Boolean keepAliveFlags) {
    if (keepAliveFlags != null) {
      for (int i : userIndices) {
        getProxyCaches(i).close(keepAliveFlags);
      }

    } else {
      for (int i : userIndices) {
        getProxyCaches(i).close();
      }
    }
  }
}
