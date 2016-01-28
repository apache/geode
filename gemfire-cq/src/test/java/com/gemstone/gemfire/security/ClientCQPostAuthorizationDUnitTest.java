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

import java.util.Collection;
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
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.query.internal.cq.ClientCQImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
/**
 * This is for multiuser-authentication
 * 
 * @author ashetkar
 *
 */
public class ClientCQPostAuthorizationDUnitTest extends
    ClientAuthorizationTestBase {

//  public static final String regionName = "ClientCQPostAuthorizationDUnitTest_region";

  public static final Map<String, String> cqNameToQueryStrings = new HashMap<String, String>();

  static {
    cqNameToQueryStrings.put("CQ_0", "SELECT * FROM ");
    cqNameToQueryStrings.put("CQ_1", "SELECT * FROM ");
  }

  public ClientCQPostAuthorizationDUnitTest(String name) {
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

  public void tearDown2() throws Exception {
    super.tearDown2();
    client1.invoke(SecurityTestUtil.class, "closeCache");
    client2.invoke(SecurityTestUtil.class, "closeCache");
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");
  }

  public void testAllowCQForAllMultiusers() throws Exception {
    /*
     * Start a server
     * Start a client1 with two users with valid credentials and post-authz'ed for CQ
     * Each user registers a unique CQ
     * Client2 does some operations on the region which satisfies both the CQs
     * Validate that listeners for both the CQs are invoked.
     */
    doStartUp(Integer.valueOf(2), Integer.valueOf(5), new Boolean[] {true,
      true});
  }

  public void testDisallowCQForAllMultiusers() throws Exception {
    /*
     * Start a server
     * Start a client1 with two users with valid credentials but not post-authz'ed for CQ
     * Each user registers a unique CQ
     * Client2 does some operations on the region which satisfies both the CQs
     * Validate that listeners for none of the CQs are invoked.
     */
    doStartUp(Integer.valueOf(2), Integer.valueOf(5), new Boolean[] {false,
      false});
  }

  public void testDisallowCQForSomeMultiusers() throws Exception {
    /*
     * Start a server
     * Start a client1 with two users with valid credentials
     * User1 is post-authz'ed for CQ but user2 is not.
     * Each user registers a unique CQ
     * Client2 does some operations on the region which satisfies both the CQs
     * Validate that listener for User1's CQ is invoked but that for User2's CQ is not invoked.
     */
    doStartUp(Integer.valueOf(2), Integer.valueOf(5), new Boolean[] {true,
        false});
  }

  public void testAllowCQForAllMultiusersWithFailover() throws Exception {
    /*
     * Start a server1
     * Start a client1 with two users with valid credentials and post-authz'ed for CQ
     * Each user registers a unique CQ
     * Client2 does some operations on the region which satisfies both the CQs
     * Validate that listeners for both the CQs are invoked.
     * Start server2 and shutdown server1
     * Client2 does some operations on the region which satisfies both the CQs
     * Validate that listeners for both the CQs are get updates.
     */
    doStartUp(Integer.valueOf(2), Integer.valueOf(5), new Boolean[] {true,
      true}, Boolean.TRUE);
  }

  public void doStartUp(Integer numOfUsers, Integer numOfPuts,
      Boolean[] postAuthzAllowed) throws Exception {
    doStartUp(numOfUsers, numOfPuts, postAuthzAllowed, Boolean.FALSE /* failover */);
  }

  public void doStartUp(Integer numOfUsers, Integer numOfPuts,
      Boolean[] postAuthzAllowed, Boolean failover) throws Exception {
      AuthzCredentialGenerator gen = this.getXmlAuthzGenerator();
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
      for (int i = 0; i < numOfUsers; i++) {
        int rand = rnd.nextInt(100) + 1;
        if (postAuthzAllowed[i]) {
          opCredentials = tgen.getAllowedCredentials(new OperationCode[] {
              OperationCode.EXECUTE_CQ, OperationCode.GET}, // For callback, GET should be allowed
              new String[] {regionName}, indices, rand);
//          authProps[i] = gen.getAllowedCredentials(
//              new OperationCode[] {OperationCode.EXECUTE_CQ},
//              new String[] {regionName}, rnd.nextInt(100) + 1);
        } else {
          opCredentials = tgen.getDisallowedCredentials(new OperationCode[] {
              OperationCode.GET}, // For callback, GET should be disallowed
              new String[] {regionName}, indices, rand);
//          authProps[i] = gen.getDisallowedCredentials(
//              new OperationCode[] {OperationCode.EXECUTE_CQ},
//              new String[] {regionName}, rnd.nextInt(100) + 1);
        }
        authProps[i] = SecurityTestUtil.concatProperties(new Properties[] {
            opCredentials, extraAuthProps, extraAuthzProps});
      }

      // Get ports for the servers
      Integer port1 = Integer.valueOf(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
      Integer port2 = Integer.valueOf(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
      Integer locatorPort = Integer.valueOf(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
      // Close down any running servers
      server1.invoke(SecurityTestUtil.class, "closeCache");
      server2.invoke(SecurityTestUtil.class, "closeCache");

      server1.invoke(ClientCQPostAuthorizationDUnitTest.class,
          "createServerCache", new Object[] {serverProps, javaProps, locatorPort, port1});
      client1.invoke(ClientCQPostAuthorizationDUnitTest.class,
          "createClientCache", new Object[] {javaProps2, authInit, authProps,
              new Integer[] {port1, port2}, numOfUsers, postAuthzAllowed});
      client2.invoke(ClientCQPostAuthorizationDUnitTest.class,
          "createClientCache", new Object[] {javaProps2, authInit, authProps,
              new Integer[] {port1, port2}, numOfUsers, postAuthzAllowed});

      client1.invoke(ClientCQPostAuthorizationDUnitTest.class, "createCQ",
          new Object[] {numOfUsers});
      client1.invoke(ClientCQPostAuthorizationDUnitTest.class, "executeCQ",
          new Object[] {numOfUsers, new Boolean[] {false, false}, numOfPuts,
              new String[numOfUsers], postAuthzAllowed});

      client2.invoke(ClientCQPostAuthorizationDUnitTest.class, "doPuts",
          new Object[] {numOfPuts, Boolean.TRUE/* put last key */});
      if (!postAuthzAllowed[0]) {
        // There is no point waiting as no user is authorized to receive cq events.
        try {Thread.sleep(1000);} catch (InterruptedException ie) {}
      } else {
        client1.invoke(ClientCQPostAuthorizationDUnitTest.class,
            "waitForLastKey", new Object[] {Integer.valueOf(0)});
        if (postAuthzAllowed[1]) {
          client1.invoke(ClientCQPostAuthorizationDUnitTest.class,
              "waitForLastKey", new Object[] {Integer.valueOf(1)});
        }
      }
      client1.invoke(ClientCQPostAuthorizationDUnitTest.class,
          "checkCQListeners", new Object[] {numOfUsers, postAuthzAllowed,
              numOfPuts + 1/* last key */, 0, !failover});
      if (failover) {
        server2.invoke(ClientCQPostAuthorizationDUnitTest.class,
            "createServerCache", new Object[] {serverProps, javaProps, locatorPort, port2});
        server1.invoke(SecurityTestUtil.class, "closeCache");

        // Allow time for client1 to register its CQs on server2
        server2.invoke(ClientCQPostAuthorizationDUnitTest.class,
            "allowCQsToRegister", new Object[] {Integer.valueOf(2)});

        client2.invoke(ClientCQPostAuthorizationDUnitTest.class, "doPuts",
            new Object[] {numOfPuts, Boolean.TRUE/* put last key */});
        client1.invoke(ClientCQPostAuthorizationDUnitTest.class,
            "waitForLastKeyUpdate", new Object[] {Integer.valueOf(0)});
        client1.invoke(ClientCQPostAuthorizationDUnitTest.class,
            "checkCQListeners", new Object[] {numOfUsers, postAuthzAllowed,
                numOfPuts + 1/* last key */, numOfPuts + 1/* last key */,
                Boolean.TRUE});
      }
  }

  public static void createServerCache(Properties serverProps,
      Properties javaProps, Integer serverPort) {
    Integer locatorPort = Integer.valueOf(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));
    SecurityTestUtil.createCacheServer((Properties)serverProps, javaProps,
        locatorPort, null, serverPort, Boolean.TRUE, Integer.valueOf(
            SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createServerCache(Properties serverProps,
        Properties javaProps, Integer locatorPort, Integer serverPort) {
    SecurityTestUtil.createCacheServer((Properties)serverProps, javaProps,
        locatorPort, null, serverPort, Boolean.TRUE, Integer.valueOf(
            SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createClientCache(Properties javaProps, String authInit,
      Properties[] authProps, Integer ports[], Integer numOfUsers,
      Boolean[] postAuthzAllowed) {
    SecurityTestUtil.createCacheClientForMultiUserMode(numOfUsers, authInit,
        authProps, javaProps, ports, null, Boolean.FALSE,
        SecurityTestUtil.NO_EXCEPTION);
  }

  public static void createCQ(Integer num) {
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
        CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
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
      Integer expectedResultsSize, String[] expectedErr, Boolean[] postAuthzAllowed) {
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
          } catch (CqException ce) {
            if (ce.getCause() instanceof NotAuthorizedException && !postAuthzAllowed[i]) {
              getLogWriter().info("Got expected exception for CQ " + cqName);
            } else {
              getLogWriter().info("CqService is: " + cqService);
              ce.printStackTrace();
              AssertionError err = new AssertionError("Failed to execute CQ "
                  + cqName);
              err.initCause(ce);
              throw err;
            }
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
          } catch (CqException ce) {
            if (ce.getCause() instanceof NotAuthorizedException && !postAuthzAllowed[i]) {
              getLogWriter().info("Got expected exception for CQ " + cqName);
            } else {
              getLogWriter().info("CqService is: " + cqService);
              ce.printStackTrace();
              AssertionError err = new AssertionError("Failed to execute CQ "
                  + cqName);
              err.initCause(ce);
              throw err;
            }
          } catch (Exception ex) {
            AssertionError err = new AssertionError("Failed to execute CQ "
                + cqName);
            err.initCause(ex);
            if (expectedErr == null) {
              getLogWriter().info("CqService is: " + cqService, err);
            }
            throw err;
          }
          assertTrue("execute() state mismatch", cq1.getState().isRunning() == postAuthzAllowed[i]);
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
//    Region region = GemFireCache.getInstance().getRegion(regionName);
    Region region = SecurityTestUtil.proxyCaches[0].getRegion(regionName);
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

  public static void waitForLastKey(Integer cqIndex) {
    String cqName = "CQ_" + cqIndex;
    QueryService qService = SecurityTestUtil.proxyCaches[cqIndex].getQueryService();
    ClientCQImpl cqQuery = (ClientCQImpl)qService.getCq(cqName);
    ((CqQueryTestListener)cqQuery.getCqListeners()[0])
        .waitForCreated("LAST_KEY");
//    WaitCriterion wc = new WaitCriterion() {
//      public boolean done() {
//        Region region = GemFireCache.getInstance().getRegion(regionName);
//        Region.Entry entry = region.getEntry("LAST_KEY");
//        if (entry != null && entry.getValue() != null) {
//          return false;
//        } else if (entry.getValue() != null) {
//          return true;
//        }
//        return false;
//      }
//      public String description() {
//        return "Last key not received.";
//      }
//    };
//    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 100, false);
  }

  public static void waitForLastKeyUpdate(Integer cqIndex) {
    String cqName = "CQ_" + cqIndex;
    QueryService qService = SecurityTestUtil.proxyCaches[cqIndex].getQueryService();
    ClientCQImpl cqQuery = (ClientCQImpl)qService.getCq(cqName);
    ((CqQueryTestListener)cqQuery.getCqListeners()[0])
        .waitForUpdated("LAST_KEY");
  }

  public static void allowCQsToRegister(Integer number) {
    final int num = number;
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        CqService cqService = GemFireCacheImpl.getInstance().getCqService();
        cqService.start();
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        if (cqs != null) {
          return cqs.size() >= num;
        } else {
          return false;
        }
      }

      public String description() {
        return num + "Waited for " + num
            + " CQs to be registered on this server.";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 100, false);
  }

  public static void checkCQListeners(Integer numOfUsers,
      Boolean[] expectedListenerInvocation, Integer createEventsSize,
      Integer updateEventsSize, Boolean closeCache) {
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
      if (closeCache) {
        SecurityTestUtil.proxyCaches[i].close();
      }
    }
  }
}
