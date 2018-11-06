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

import static org.apache.geode.internal.cache.GemFireCacheImpl.getInstance;
import static org.apache.geode.security.SecurityTestUtils.NO_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.REGION_NAME;
import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.security.SecurityTestUtils.concatProperties;
import static org.apache.geode.security.SecurityTestUtils.createCacheClientForMultiUserMode;
import static org.apache.geode.security.SecurityTestUtils.getProxyCaches;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.Collection;
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
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * This is for multiuser-authentication
 */
@Category({SecurityTest.class})
public class ClientCQPostAuthorizationDUnitTest extends ClientAuthorizationTestCase {

  private Map<String, String> cqNameToQueryStrings = new HashMap<>();

  @Override
  protected final void preSetUpClientAuthorizationTestBase() throws Exception {
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    this.cqNameToQueryStrings.put("CQ_0", "SELECT * FROM ");
    this.cqNameToQueryStrings.put("CQ_1", "SELECT * FROM ");
  }

  @Override
  public final void postTearDownClientAuthorizationTestBase() throws Exception {
    this.cqNameToQueryStrings.clear();
  }

  @Test
  public void testAllowCQForAllMultiusers() throws Exception {
    /*
     * Start a server Start a client1 with two users with valid credentials and post-authz'ed for CQ
     * Each user registers a unique CQ Client2 does some operations on the region which satisfies
     * both the CQs Validate that listeners for both the CQs are invoked.
     */
    doStartUp(2, 5, new boolean[] {true, true}, false);
  }

  @Test
  public void testDisallowCQForAllMultiusers() throws Exception {
    /*
     * Start a server Start a client1 with two users with valid credentials but not post-authz'ed
     * for CQ Each user registers a unique CQ Client2 does some operations on the region which
     * satisfies both the CQs Validate that listeners for none of the CQs are invoked.
     */
    doStartUp(2, 5, new boolean[] {false, false}, false);
  }

  @Test
  public void testDisallowCQForSomeMultiusers() throws Exception {
    /*
     * Start a server Start a client1 with two users with valid credentials User1 is post-authz'ed
     * for CQ but user2 is not. Each user registers a unique CQ Client2 does some operations on the
     * region which satisfies both the CQs Validate that listener for User1's CQ is invoked but that
     * for User2's CQ is not invoked.
     */
    doStartUp(2, 5, new boolean[] {true, false}, false);
  }

  @Test
  public void testAllowCQForAllMultiusersWithFailover() throws Exception {
    /*
     * Start a server1 Start a client1 with two users with valid credentials and post-authz'ed for
     * CQ Each user registers a unique CQ Client2 does some operations on the region which satisfies
     * both the CQs Validate that listeners for both the CQs are invoked. Start server2 and shutdown
     * server1 Client2 does some operations on the region which satisfies both the CQs Validate that
     * listeners for both the CQs are get updates.
     */
    doStartUp(2, 5, new boolean[] {true, true}, true);
  }

  private void doStartUp(final int numOfUsers, final int numOfPuts,
      final boolean[] postAuthzAllowed, final boolean failover) throws Exception {
    AuthzCredentialGenerator authzGenerator = getXmlAuthzGenerator();
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
        credentialGenerator == null ? null : credentialGenerator.getJavaProperties();

    int[] indices = new int[numOfPuts];
    for (int index = 0; index < numOfPuts; ++index) {
      indices[index] = index;
    }

    Random rnd = new Random();
    Properties[] authProps = new Properties[numOfUsers];
    for (int i = 0; i < numOfUsers; i++) {
      int rand = rnd.nextInt(100) + 1;

      if (postAuthzAllowed[i]) {
        // For callback, GET should be allowed
        opCredentials = tgen.getAllowedCredentials(
            new OperationCode[] {OperationCode.EXECUTE_CQ, OperationCode.GET},
            new String[] {REGION_NAME}, indices, rand);
      } else {
        // For callback, GET should be disallowed
        opCredentials = tgen.getDisallowedCredentials(new OperationCode[] {OperationCode.GET},
            new String[] {REGION_NAME}, indices, rand);
      }

      authProps[i] =
          concatProperties(new Properties[] {opCredentials, extraAuthProps, extraAuthzProps});
    }

    // Get ports for the servers
    int[] randomAvailableTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = randomAvailableTCPPorts[0];
    int port2 = randomAvailableTCPPorts[1];

    // Close down any running servers
    server1.invoke(() -> closeCache());
    server2.invoke(() -> closeCache());

    server1.invoke(() -> createTheServerCache(serverProps, javaProps, port1));
    client1.invoke(() -> createClientCache(javaProps2, authInit, authProps,
        new int[] {port1, port2}, numOfUsers, postAuthzAllowed));
    client2.invoke(() -> createClientCache(javaProps2, authInit, authProps,
        new int[] {port1, port2}, numOfUsers, postAuthzAllowed));

    client1.invoke(() -> createCQ(numOfUsers));
    client1.invoke(() -> executeCQ(numOfUsers, new boolean[] {false, false}, numOfPuts,
        new String[numOfUsers], postAuthzAllowed));

    client2.invoke(() -> doPuts(numOfPuts, true/* put last key */));

    if (!postAuthzAllowed[0]) {
      // There is no point waiting as no user is authorized to receive cq events.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      } // TODO: replace with Awaitility
    } else {
      client1.invoke(() -> waitForLastKey(0));
      if (postAuthzAllowed[1]) {
        client1.invoke(() -> waitForLastKey(1));
      }
    }

    client1.invoke(() -> checkCQListeners(numOfUsers, postAuthzAllowed, numOfPuts + 1/* last key */,
        0, !failover));

    if (failover) {
      server2.invoke(() -> createTheServerCache(serverProps, javaProps, port2));
      server1.invoke(() -> closeCache());

      // Allow time for client1 to register its CQs on server2
      server2.invoke(() -> allowCQsToRegister(2));

      client2.invoke(() -> doPuts(numOfPuts, true/* put last key */));
      client1.invoke(() -> waitForLastKeyUpdate(0));
      client1.invoke(() -> checkCQListeners(numOfUsers, postAuthzAllowed,
          numOfPuts + 1/* last key */, numOfPuts + 1/* last key */, true));
    }
  }

  private void createTheServerCache(final Properties serverProps, final Properties javaProps,
      final int serverPort) {
    SecurityTestUtils.createCacheServer(serverProps, javaProps, serverPort, true, NO_EXCEPTION);
  }

  private void createClientCache(final Properties javaProps, final String authInit,
      final Properties[] authProps, final int ports[], final int numOfUsers,
      final boolean[] postAuthzAllowed) {
    createCacheClientForMultiUserMode(numOfUsers, authInit, authProps, javaProps, ports, 0, false,
        NO_EXCEPTION);
  }

  private void createCQ(final int num) throws CqException, CqExistsException {
    for (int i = 0; i < num; i++) {
      QueryService cqService = getProxyCaches(i).getQueryService();
      String cqName = "CQ_" + i;
      String queryStr =
          cqNameToQueryStrings.get(cqName) + getProxyCaches(i).getRegion(REGION_NAME).getFullPath();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
      ((CqQueryTestListener) cqListeners[0]).cqName = cqName;

      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      // Create CQ.
      CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
    }
  }

  private void executeCQ(final int num, final boolean[] initialResults,
      final int expectedResultsSize, final String[] expectedErr, final boolean[] postAuthzAllowed)
      throws RegionNotFoundException, CqException {
    for (int i = 0; i < num; i++) {
      try {
        if (expectedErr[i] != null) {
          getLogWriter()
              .info("<ExpectedException action=add>" + expectedErr[i] + "</ExpectedException>");
        }
        CqQuery cq1 = null;
        String cqName = "CQ_" + i;
        // String queryStr = cqNameToQueryStrings.get(cqName) +
        // getProxyCaches(i).getRegion(REGION_NAME).getFullPath();
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

          try {
            cqResults = cq1.executeWithInitialResults();
          } catch (CqException ce) {
            if (ce.getCause() instanceof NotAuthorizedException && !postAuthzAllowed[i]) {
              getLogWriter().info("Got expected exception for CQ " + cqName);
            } else {
              getLogWriter().info("CqService is: " + cqService);
              throw new AssertionError("Failed to execute CQ " + cqName, ce);
            }
          }

          getLogWriter().info("initial result size = " + cqResults.size());
          assertTrue("executeWithInitialResults() state mismatch", cq1.getState().isRunning());
          if (expectedResultsSize >= 0) {
            assertEquals("unexpected results size", expectedResultsSize, cqResults.size());
          }

        } else {

          try {
            cq1.execute();
          } catch (CqException ce) {
            if (ce.getCause() instanceof NotAuthorizedException && !postAuthzAllowed[i]) {
              getLogWriter().info("Got expected exception for CQ " + cqName);
            } else {
              throw ce;
            }
          }

          assertTrue("execute() state mismatch", cq1.getState().isRunning() == postAuthzAllowed[i]);
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
    Region region = getProxyCaches(0).getRegion(REGION_NAME);
    for (int i = 0; i < num; i++) {
      region.put("CQ_key" + i, "CQ_value" + i);
    }
    if (putLastKey) {
      region.put("LAST_KEY", "LAST_KEY");
    }
  }

  private void waitForLastKey(final int cqIndex) {
    String cqName = "CQ_" + cqIndex;
    QueryService qService = getProxyCaches(cqIndex).getQueryService();
    ClientCQImpl cqQuery = (ClientCQImpl) qService.getCq(cqName);
    ((CqQueryTestListener) cqQuery.getCqListeners()[0]).waitForCreated("LAST_KEY");
  }

  private void waitForLastKeyUpdate(final int cqIndex) {
    String cqName = "CQ_" + cqIndex;
    QueryService qService = getProxyCaches(cqIndex).getQueryService();
    ClientCQImpl cqQuery = (ClientCQImpl) qService.getCq(cqName);
    ((CqQueryTestListener) cqQuery.getCqListeners()[0]).waitForUpdated("LAST_KEY");
  }

  private void allowCQsToRegister(final int number) {
    final int num = number;
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        CqService cqService = getInstance().getCqService();
        cqService.start();
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        if (cqs != null) {
          return cqs.size() >= num;
        } else {
          return false;
        }
      }

      @Override
      public String description() {
        return num + "Waited for " + num + " CQs to be registered on this server.";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  private boolean checkCQListeners(final int numOfUsers, final boolean[] expectedListenerInvocation,
      final int createEventsSize, final int updateEventsSize, final boolean closeCache) {
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
      if (closeCache) {
        getProxyCaches(i).close();
      }
    }
    return true;
  }
}
