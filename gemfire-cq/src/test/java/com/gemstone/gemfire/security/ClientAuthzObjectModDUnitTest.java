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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import security.CredentialGenerator;
import security.DummyAuthzCredentialGenerator;
import security.DummyCredentialGenerator;
import templates.security.UserPasswordAuthInit;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.security.ObjectWithAuthz;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * Tests for authorization callback that modify objects and callbacks from
 * client to server.
 * 
 * The scheme of these tests is thus: A user name terminating in integer 'i' can
 * get values terminating with 'i', '2*i', '3*i' and so on. So 'gemfire1' can
 * get 'value1', 'value2', ...; 'gemfire2' can get 'value2', 'value4', ... and
 * so on. On the server side this is done by adding the index 'i' to the object
 * in the pre-processing phase, and checked by comparing against the user name
 * index during the post-processing phase.
 * 
 * This enables testing of object and callback modification both in
 * pre-processing and post-processing phases.
 * 
 * @author sumedh
 * @since 5.5
 */
public class ClientAuthzObjectModDUnitTest extends ClientAuthorizationTestBase {


  /** constructor */
  public ClientAuthzObjectModDUnitTest(String name) {
    super(name);
  }

  private static final String preAccessor = "com.gemstone.gemfire.internal."
      + "security.FilterPreAuthorization.create";

  private static final String postAccessor = "com.gemstone.gemfire.internal."
      + "security.FilterPostAuthorization.create";

  private static class TestPostCredentialGenerator implements
      TestCredentialGenerator {

    public TestPostCredentialGenerator() {
    }

    public Properties getAllowedCredentials(OperationCode[] opCodes,
        String[] regionNames, int[] keyIndices, int num) {

      int userIndex = 1;
      byte role = DummyAuthzCredentialGenerator.getRequiredRole(opCodes);
      if (role == DummyAuthzCredentialGenerator.READER_ROLE) {
        userIndex = keyIndices[0] + 1;
      }
      Properties props = new Properties();
      props.setProperty(UserPasswordAuthInit.USER_NAME, "user" + userIndex);
      props.setProperty(UserPasswordAuthInit.PASSWORD, "user" + userIndex);
      return props;
    }

    public Properties getDisallowedCredentials(OperationCode[] opCodes,
        String[] regionNames, int[] keyIndices, int num) {

      int userIndex = 0;
      for (int index = 0; index < keyIndices.length; ++index) {
        if (keyIndices[index] != index) {
          userIndex = index + 1;
          break;
        }
      }
      Properties props = new Properties();
      props.setProperty(UserPasswordAuthInit.USER_NAME, "gemfire" + userIndex);
      props.setProperty(UserPasswordAuthInit.PASSWORD, "gemfire" + userIndex);
      return props;
    }

    public CredentialGenerator getCredentialGenerator() {

      return null;
    }
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

    //required by FilterPreAuthorization and FilterPostAuthorization. Normally,
    //this would be automatically registered in the static initializer, but with dunit
    //a previous test may have already loaded these classes. We clear the instantiators
    //between each test.
    SerializableRunnable registerInstantiator = new SerializableRunnable() {
      public void run() {
        Instantiator.register(new MyInstantiator(), false);
      }
    };
    server1.invoke(registerInstantiator);
    server2.invoke(registerInstantiator);
  }
  
  public void tearDown2() throws Exception  {
    super.tearDown2();
    DistributedTestCase.cleanupAllVms();
  }

  // Region: Utility and static functions invoked by the tests

  private static Properties buildProperties(String authenticator,
      Properties extraProps, String preAccessor, String postAccessor) {

    Properties authProps = new Properties();
    if (authenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, authenticator);
    }
    if (preAccessor != null) {
      authProps.setProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME,
          preAccessor);
    }
    if (postAccessor != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME, postAccessor);
    }
    if (extraProps != null) {
      authProps.putAll(extraProps);
    }
    return authProps;
  }

  public static Integer createCacheServer(Integer mcastPort,
      Properties authProps) {

    if (mcastPort == null) {
      mcastPort = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
    }
    return SecurityTestUtil.createCacheServer(authProps, null, mcastPort, null,
        null, Boolean.FALSE, new Integer(SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createCacheServer(Integer loctorPort, Integer serverPort,
      Properties authProps) {

    if (loctorPort == null) {
      loctorPort = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
    }
    SecurityTestUtil.createCacheServer(authProps, null, loctorPort, null,
        serverPort, Boolean.FALSE, new Integer(SecurityTestUtil.NO_EXCEPTION));
  }

  // End Region: Utility and static functions invoked by the tests

  // Region: Tests

  public void testAllOpsObjectModWithFailover() {

    OperationWithAction[] allOps = {
        // Perform CREATE and verify with GET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.NONE, 8),
        // For second client connect with valid credentials for key2, key4,
        // key6, key8 and check that other keys are not accessible
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY,
            new int[] { 1, 3, 5, 7 }),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY
            | OpFlags.USE_OLDCONN | OpFlags.CHECK_NOTAUTHZ, new int[] { 0, 2,
            4, 6 }),
        // For third client check that key3, key6 are accessible but others are
        // not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY,
            new int[] { 2, 5 }),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY
            | OpFlags.USE_OLDCONN | OpFlags.CHECK_NOTAUTHZ, new int[] { 0, 1,
            3, 4, 6, 7 }),

        // OPBLOCK_END indicates end of an operation block that needs to
        // be executed on each server when doing failover
        OperationWithAction.OPBLOCK_END,

        // Perform UPDATE and verify with GET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible
        // but others are not
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, new int[] { 1, 3, 5, 7 }),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_NOTAUTHZ, new int[] { 0, 2, 4, 6 }),
        // For third client check that key3, key6 are accessible but others are
        // not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, new int[] { 2, 5 }),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_NOTAUTHZ, new int[] { 0, 1, 3, 4, 6, 7 }),

        OperationWithAction.OPBLOCK_END,

        // Perform UPDATE and verify with GET_ALL
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible
        // but others are not; getAll test in doOp uses a combination of local
        // entries and remote fetches
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS,
            new int[] { 1, 3, 5, 7 }),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS | OpFlags.CHECK_NOKEY
            | OpFlags.CHECK_FAIL, new int[] { 0, 2, 4, 6 }),
        // For third client check that key3, key6 are accessible but others are
        // not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS, new int[] { 2, 5 }),
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS | OpFlags.CHECK_NOKEY
            | OpFlags.CHECK_FAIL, new int[] { 0, 1, 3, 4, 6, 7 }),

        // locally destroy the keys to also test create after failover
        new OperationWithAction(OperationCode.DESTROY, 1, OpFlags.USE_OLDCONN
            | OpFlags.LOCAL_OP, 8),

        OperationWithAction.OPBLOCK_END,

        // Perform PUTALL and verify with GET
        new OperationWithAction(OperationCode.PUTALL, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible
        // but others are not
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, new int[] { 1, 3, 5, 7 }),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_NOTAUTHZ, new int[] { 0, 2, 4, 6 }),
        // For third client check that key3, key6 are accessible but others are
        // not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, new int[] { 2, 5 }),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_NOTAUTHZ, new int[] { 0, 1, 3, 4, 6, 7 }),

        OperationWithAction.OPBLOCK_END,
        
        // Test UPDATE and verify with a QUERY
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),
        // For second client check that key2, key4, key6, key8 are accessible
        // but others are not
        new OperationWithAction(OperationCode.QUERY, 2, OpFlags.USE_OLDCONN,
            new int[] { 1, 3, 5, 7 }),
        new OperationWithAction(OperationCode.QUERY, 2, OpFlags.USE_OLDCONN
            | OpFlags.CHECK_FAIL, new int[] { 0, 2, 4, 6 }),
        // For third client check that key3, key6 are accessible but others are
        // not
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.USE_OLDCONN,
            new int[] { 2, 5 }),
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.USE_OLDCONN
            | OpFlags.CHECK_FAIL, new int[] { 0, 1, 3, 4, 6, 7 }),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a EXECUTE_CQ initial results
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN
            | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible
        // but others are not
        new OperationWithAction(OperationCode.EXECUTE_CQ, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, new int[] { 1, 3, 5, 7 }),
        new OperationWithAction(OperationCode.CLOSE_CQ, 2, OpFlags.USE_OLDCONN,
            1),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_FAIL,
            new int[] { 0, 2, 4, 6 }),
        new OperationWithAction(OperationCode.CLOSE_CQ, 2, OpFlags.USE_OLDCONN,
            1),
        // For third client check that key3, key6 are accessible but others are
        // not
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, new int[] { 2, 5 }),
        new OperationWithAction(OperationCode.CLOSE_CQ, 3, OpFlags.USE_OLDCONN,
            1),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_FAIL,
            new int[] { 0, 1, 3, 4, 6, 7 }),
        new OperationWithAction(OperationCode.CLOSE_CQ, 3, OpFlags.USE_OLDCONN,
            1),

        OperationWithAction.OPBLOCK_END };

    TestPostCredentialGenerator tgen = new TestPostCredentialGenerator();

    CredentialGenerator gen = new DummyCredentialGenerator();
    gen.init();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authInit = gen.getAuthInit();
    String authenticator = gen.getAuthenticator();

    getLogWriter().info(
        "testPutsGetsObjectModWithFailover: Using authinit: " + authInit);
    getLogWriter().info(
        "testPutsGetsObjectModWithFailover: Using authenticator: "
            + authenticator);
    getLogWriter().info(
        "testPutsGetsObjectModWithFailover: Using pre-operation accessor: "
            + preAccessor);
    getLogWriter().info(
        "testPutsGetsObjectModWithFailover: Using post-operation accessor: "
            + postAccessor);

    // Start servers with all required properties
    Properties serverProps = buildProperties(authenticator, extraProps,
        preAccessor, postAccessor);
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
          executeOpBlock(opBlock, port1, port2, authInit, extraProps, null,
              tgen, rnd);
          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            // Failover to the second server and run the block again
            server2.invoke(ClientAuthorizationTestBase.class,
                "createCacheServer", new Object[] {
                    SecurityTestUtil.getLocatorPort(), port2, serverProps,
                    javaProps });
            server1.invoke(SecurityTestUtil.class, "closeCache");
            executeOpBlock(opBlock, port1, port2, authInit, extraProps, null,
                tgen, rnd);
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

  private static class MyInstantiator extends Instantiator {

    public MyInstantiator(Class clazz, int classId) {
      super(clazz, classId);
    }
    public MyInstantiator() {
      this(ObjectWithAuthz.class, ObjectWithAuthz.CLASSID);
    }

    public DataSerializable newInstance() {
      return new ObjectWithAuthz();
    }
  }

}
