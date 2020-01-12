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

import static org.apache.geode.security.SecurityTestUtils.closeCache;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyAuthzCredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Tests for authorization callback that modify objects and callbacks from client to server.
 *
 * The scheme of these tests is thus: A user name terminating in integer 'i' can get values
 * terminating with 'i', '2*i', '3*i' and so on. So 'gemfire1' can get 'value1', 'value2', ...;
 * 'gemfire2' can get 'value2', 'value4', ... and so on. On the server side this is done by adding
 * the index 'i' to the object in the pre-processing phase, and checked by comparing against the
 * user name index during the post-processing phase.
 *
 * This enables testing of object and callback modification both in pre-processing and
 * post-processing phases.
 *
 * @since GemFire 5.5
 */
@Category({SecurityTest.class})
public class ClientAuthzObjectModDUnitTest extends ClientAuthorizationTestCase {

  private static final String PRE_ACCESSOR_CREATE =
      FilterPreAuthorization.class.getName() + ".create";
  private static final String POST_ACCESSOR_CREATE =
      FilterPostAuthorization.class.getName() + ".create";

  @Override
  protected final void postSetUpClientAuthorizationTestBase() throws Exception {
    // required by FilterPreAuthorization and FilterPostAuthorization. Normally,
    // this would be automatically registered in the static initializer, but with dunit
    // a previous test may have already loaded these classes. We clear the instantiators
    // between each test.
    server1.invoke("registerInstantiator",
        () -> Instantiator.register(new MyInstantiator(), false));
    server2.invoke("registerInstantiator",
        () -> Instantiator.register(new MyInstantiator(), false));
  }

  @Test
  public void testAllOpsObjectModWithFailover() throws Exception {
    OperationWithAction[] allOps = allOps();

    TestPostCredentialGenerator tgen = new TestPostCredentialGenerator();

    CredentialGenerator credentialGenerator = new DummyCredentialGenerator();
    credentialGenerator.init();
    Properties extraProps = credentialGenerator.getSystemProperties();
    Properties javaProps = credentialGenerator.getJavaProperties();
    String authInit = credentialGenerator.getAuthInit();
    String authenticator = credentialGenerator.getAuthenticator();

    System.out.println("testPutsGetsObjectModWithFailover: Using authinit: " + authInit);
    System.out.println("testPutsGetsObjectModWithFailover: Using authenticator: " + authenticator);
    System.out.println(
        "testPutsGetsObjectModWithFailover: Using pre-operation accessor: " + PRE_ACCESSOR_CREATE);
    System.out.println("testPutsGetsObjectModWithFailover: Using post-operation accessor: "
        + POST_ACCESSOR_CREATE);

    // Start servers with all required properties
    Properties serverProps =
        buildProperties(authenticator, extraProps, PRE_ACCESSOR_CREATE, POST_ACCESSOR_CREATE);

    // Get ports for the servers
    int[] portsList = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = 0; // portsList[0];
    int port2 = portsList[1];

    // Perform all the ops on the clients
    List<OperationWithAction> opBlock = new ArrayList();
    Random rnd = new Random();

    for (int opNum = 0; opNum < allOps.length; ++opNum) {
      // Start client with valid credentials as specified in OperationWithAction
      OperationWithAction currentOp = allOps[opNum];
      if (currentOp.equals(OperationWithAction.OPBLOCK_END)
          || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
        // End of current operation block; execute all the operations on the servers with failover
        if (opBlock.size() > 0) {
          // Start the first server and execute the operation block
          port1 = server1.invoke("createCacheServer",
              () -> createCacheServer(serverProps, javaProps));
          server2.invoke("closeCache", () -> closeCache());
          executeOpBlock(opBlock, port1, port2, authInit, extraProps, null, tgen, rnd);
          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            // Failover to the second server and run the block again
            port2 = server2.invoke("createCacheServer",
                () -> createCacheServer(serverProps, javaProps));
            server1.invoke("closeCache", () -> closeCache());
            executeOpBlock(opBlock, port1, port2, authInit, extraProps, null, tgen, rnd);
          }
          opBlock.clear();
        }

      } else {
        currentOp.setOpNum(opNum);
        opBlock.add(currentOp);
      }
    }
  }

  private OperationWithAction[] allOps() {
    return new OperationWithAction[] {
        // Perform CREATE and verify with GET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.NONE, 8),
        // For second client connect with valid credentials for key2, key4, key6, key8 and check
        // that other KEYS are not accessible
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY, new int[] {1, 3, 5, 7}),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.CHECK_NOKEY | OpFlags.USE_OLDCONN | OpFlags.CHECK_NOTAUTHZ,
            new int[] {0, 2, 4, 6}),
        // For third client check that key3, key6 are accessible but others are not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.CHECK_NOKEY, new int[] {2, 5}),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.CHECK_NOKEY | OpFlags.USE_OLDCONN | OpFlags.CHECK_NOTAUTHZ,
            new int[] {0, 1, 3, 4, 6, 7}),

        // OPBLOCK_END indicates end of an operation block that needs to be executed on each server
        // when doing failover
        OperationWithAction.OPBLOCK_END,

        // Perform UPDATE and verify with GET
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible but others are not
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL,
            new int[] {1, 3, 5, 7}),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY | OpFlags.CHECK_NOTAUTHZ,
            new int[] {0, 2, 4, 6}),
        // For third client check that key3, key6 are accessible but others are not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL,
            new int[] {2, 5}),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY | OpFlags.CHECK_NOTAUTHZ,
            new int[] {0, 1, 3, 4, 6, 7}),

        OperationWithAction.OPBLOCK_END,

        // Perform UPDATE and verify with GET_ALL
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible but others are not;
        // getAll test in doOp uses a combination of local entries and remote fetches
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS,
            new int[] {1, 3, 5, 7}),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_FAIL,
            new int[] {0, 2, 4, 6}),
        // For third client check that key3, key6 are accessible but others are not
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS, new int[] {2, 5}),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.USE_ALL_KEYS | OpFlags.CHECK_NOKEY
                | OpFlags.CHECK_FAIL,
            new int[] {0, 1, 3, 4, 6, 7}),

        // locally destroy the KEYS to also test create after failover
        new OperationWithAction(OperationCode.DESTROY, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP,
            8),

        OperationWithAction.OPBLOCK_END,

        // Perform PUTALL and verify with GET
        new OperationWithAction(OperationCode.PUTALL, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL,
            8),
        // For second client check that key2, key4, key6, key8 are accessible but others are not
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL,
            new int[] {1, 3, 5, 7}),
        new OperationWithAction(OperationCode.GET, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY | OpFlags.CHECK_NOTAUTHZ,
            new int[] {0, 2, 4, 6}),
        // For third client check that key3, key6 are accessible but others are not
        new OperationWithAction(OperationCode.GET, 3, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL,
            new int[] {2, 5}),
        new OperationWithAction(OperationCode.GET, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_NOKEY | OpFlags.CHECK_NOTAUTHZ,
            new int[] {0, 1, 3, 4, 6, 7}),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a QUERY
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN, 8),
        // For second client check that key2, key4, key6, key8 are accessible but others are not
        new OperationWithAction(OperationCode.QUERY, 2, OpFlags.USE_OLDCONN,
            new int[] {1, 3, 5, 7}),
        new OperationWithAction(OperationCode.QUERY, 2, OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL,
            new int[] {0, 2, 4, 6}),
        // For third client check that key3, key6 are accessible but others are not
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.USE_OLDCONN, new int[] {2, 5}),
        new OperationWithAction(OperationCode.QUERY, 3, OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL,
            new int[] {0, 1, 3, 4, 6, 7}),

        OperationWithAction.OPBLOCK_END,

        // Test UPDATE and verify with a EXECUTE_CQ initial results
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 8),
        // For second client check that key2, key4, key6, key8 are accessible but others are not
        new OperationWithAction(OperationCode.EXECUTE_CQ, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, new int[] {1, 3, 5, 7}),
        new OperationWithAction(OperationCode.CLOSE_CQ, 2, OpFlags.USE_OLDCONN, 1),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 2,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_FAIL, new int[] {0, 2, 4, 6}),
        new OperationWithAction(OperationCode.CLOSE_CQ, 2, OpFlags.USE_OLDCONN, 1),
        // For third client check that key3, key6 are accessible but others are not
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, new int[] {2, 5}),
        new OperationWithAction(OperationCode.CLOSE_CQ, 3, OpFlags.USE_OLDCONN, 1),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3,
            OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL | OpFlags.CHECK_FAIL,
            new int[] {0, 1, 3, 4, 6, 7}),
        new OperationWithAction(OperationCode.CLOSE_CQ, 3, OpFlags.USE_OLDCONN, 1),

        OperationWithAction.OPBLOCK_END};
  }

  private Properties buildProperties(final String authenticator, final Properties extraProps,
      final String preAccessor, final String postAccessor) {
    Properties authProps = new Properties();
    if (authenticator != null) {
      authProps.setProperty(ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR, authenticator);
    }
    if (preAccessor != null) {
      authProps.setProperty(ConfigurationProperties.SECURITY_CLIENT_ACCESSOR, preAccessor);
    }
    if (postAccessor != null) {
      authProps.setProperty(ConfigurationProperties.SECURITY_CLIENT_ACCESSOR_PP, postAccessor);
    }
    if (extraProps != null) {
      authProps.putAll(extraProps);
    }
    return authProps;
  }

  private static class MyInstantiator extends Instantiator {

    public MyInstantiator() {
      this(ObjectWithAuthz.class, ObjectWithAuthz.CLASSID);
    }

    public MyInstantiator(final Class clazz, final int classId) {
      super(clazz, classId);
    }

    @Override
    public DataSerializable newInstance() {
      return new ObjectWithAuthz();
    }
  }

  private static class TestPostCredentialGenerator implements TestCredentialGenerator {

    public TestPostCredentialGenerator() {}

    @Override
    public Properties getAllowedCredentials(final OperationCode[] opCodes,
        final String[] regionNames, final int[] keyIndices, final int num) {
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

    @Override
    public Properties getDisallowedCredentials(final OperationCode[] opCodes,
        final String[] regionNames, final int[] keyIndices, final int num) {
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

    @Override
    public CredentialGenerator getCredentialGenerator() {
      return null;
    }
  }
}
