
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


import java.util.Properties;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.internal.cache.PartitionedRegionLocalMaxMemoryDUnitTest.TestObject1;
import com.gemstone.gemfire.security.generator.AuthzCredentialGenerator;
import com.gemstone.gemfire.security.generator.CredentialGenerator;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;

/**
 * @since 6.1
 */
public class DeltaClientAuthorizationDUnitTest extends
    ClientAuthorizationTestBase {

  protected static final DeltaTestImpl[] deltas = new DeltaTestImpl[8];

  static {
    for (int i = 0; i < 8; i++) {
      deltas[i] = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObject1("0", 0));
    }
    deltas[1].setIntVar(5);
    deltas[2].setIntVar(5);
    deltas[3].setIntVar(5);
    deltas[4].setIntVar(5);
    deltas[5].setIntVar(5);
    deltas[6].setIntVar(5);
    deltas[7].setIntVar(5);

    deltas[2].resetDeltaStatus();
    deltas[2].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltas[3].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltas[4].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltas[5].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    //deltas[6].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    //deltas[7].setByteArr(new byte[] { 1, 2, 3, 4, 5 });

    deltas[3].resetDeltaStatus();
    deltas[3].setDoubleVar(new Double(5));
    deltas[4].setDoubleVar(new Double(5));
    deltas[5].setDoubleVar(new Double(5));
    deltas[6].setDoubleVar(new Double(5));
    deltas[7].setDoubleVar(new Double(5));

    deltas[4].resetDeltaStatus();
    deltas[4].setStr("str changed");
    deltas[5].setStr("str changed");
    deltas[6].setStr("str changed");
    //deltas[7].setStr("str changed");

    deltas[5].resetDeltaStatus();
    deltas[5].setIntVar(100);
    deltas[5].setTestObj(new TestObject1("CHANGED", 100));
    deltas[6].setTestObj(new TestObject1("CHANGED", 100));
    deltas[7].setTestObj(new TestObject1("CHANGED", 100));

    deltas[6].resetDeltaStatus();
    deltas[6].setByteArr(new byte[] { 1, 2, 3 });
    deltas[7].setByteArr(new byte[] { 1, 2, 3 });

    deltas[7].resetDeltaStatus();
    deltas[7].setStr("delta string");
    
  }

  /** constructor */
  public DeltaClientAuthorizationDUnitTest(String name) {
    super(name);
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);

    server1.invoke(() -> SecurityTestUtil.registerExpectedExceptions( serverExpectedExceptions ));
    server2.invoke(() -> SecurityTestUtil.registerExpectedExceptions( serverExpectedExceptions ));
    client2.invoke(() -> SecurityTestUtil.registerExpectedExceptions( clientExpectedExceptions ));
    SecurityTestUtil.registerExpectedExceptions(clientExpectedExceptions);
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    client1.invoke(() -> SecurityTestUtil.closeCache());
    client2.invoke(() -> SecurityTestUtil.closeCache());
    SecurityTestUtil.closeCache();
    // then close the servers
    server1.invoke(() -> SecurityTestUtil.closeCache());
    server2.invoke(() -> SecurityTestUtil.closeCache());
  }

  public void testAllowPutsGets() throws Exception {
      AuthzCredentialGenerator gen = this.getXmlAuthzGenerator();
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
      Integer port1 = createServer1(javaProps, serverProps);
      Integer port2 = createServer2(javaProps, serverProps);

      // Start client1 with valid CREATE credentials
      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      javaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter().info(
          "testAllowPutsGets: For first client credentials: "
              + createCredentials);
      createClient1(javaProps, authInit, port1, port2, createCredentials);

      // Start client2 with valid GET credentials
      Properties getCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.GET },
          new String[] { regionName }, 2);
      javaProps = cGen.getJavaProperties();
      LogWriterUtils.getLogWriter()
          .info(
              "testAllowPutsGets: For second client credentials: "
                  + getCredentials);
      createClient2(javaProps, authInit, port1, port2, getCredentials);

      // Perform some put operations from client1
      client1.invoke(() -> DeltaClientAuthorizationDUnitTest.doPuts(
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION), Boolean.FALSE ));
      Thread.sleep(5000);
      assertTrue("Delta feature NOT used", (Boolean)client1.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed()));

      // Verify that the gets succeed
      client2.invoke(() -> DeltaClientAuthorizationDUnitTest.doGets(
          new Integer(2), new Integer(SecurityTestUtil.NO_EXCEPTION), Boolean.FALSE  ));
  }

  protected void createClient2(Properties javaProps, String authInit,
      Integer port1, Integer port2, Properties getCredentials) {
    client2.invoke(() -> ClientAuthenticationDUnitTest.createCacheClient( authInit, getCredentials, javaProps, port1, port2,
            null, new Integer(SecurityTestUtil.NO_EXCEPTION) ));
  }

  protected void createClient1(Properties javaProps, String authInit,
      Integer port1, Integer port2, Properties createCredentials) {
    client1.invoke(() -> ClientAuthenticationDUnitTest.createCacheClient( authInit, createCredentials, javaProps, port1, port2,
            null, new Integer(SecurityTestUtil.NO_EXCEPTION) ));
  }

  protected Integer createServer2(Properties javaProps,
      Properties serverProps) {
    Integer port2 = ((Integer)server2.invoke(() -> ClientAuthorizationTestBase.createCacheServer(
            SecurityTestUtil.getLocatorPort(), serverProps, javaProps )));
    return port2;
  }

  protected Integer createServer1(Properties javaProps,
      Properties serverProps) {
    Integer port1 = ((Integer)server1.invoke(() -> ClientAuthorizationTestBase.createCacheServer(
            SecurityTestUtil.getLocatorPort(), serverProps, javaProps )));
    return port1;
  }

  public static void doPuts(Integer num, Integer expectedResult,
      boolean newVals) {

    assertTrue(num.intValue() <= SecurityTestUtil.keys.length);
    Region region = null;
    try {
      region = SecurityTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
        LogWriterUtils.getLogWriter().info("Got expected exception when doing puts: " + ex);
      }
      else {
        Assert.fail("Got unexpected exception when doing puts", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      region.put(SecurityTestUtil.keys[index], deltas[0]);
    }
    for (int index = 0; index < num.intValue(); ++index) {
      try {
        region.put(SecurityTestUtil.keys[index], deltas[index]);
        if (expectedResult.intValue() != SecurityTestUtil.NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing puts");
        }
      }
      catch (NoAvailableServersException ex) {
        if (expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          LogWriterUtils.getLogWriter().info(
              "Got expected NoAvailableServers when doing puts: "
                  + ex.getCause());
          continue;
        }
        else {
          Assert.fail("Got unexpected exception when doing puts", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == SecurityTestUtil.NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          LogWriterUtils.getLogWriter().info(
              "Got expected NotAuthorizedException when doing puts: "
                  + ex.getCause());
          continue;
        }
        if ((expectedResult.intValue() == SecurityTestUtil.AUTHREQ_EXCEPTION)
            && (ex.getCause() instanceof AuthenticationRequiredException)) {
          LogWriterUtils.getLogWriter().info(
              "Got expected AuthenticationRequiredException when doing puts: "
                  + ex.getCause());
          continue;
        }
        if ((expectedResult.intValue() == SecurityTestUtil.AUTHFAIL_EXCEPTION)
            && (ex.getCause() instanceof AuthenticationFailedException)) {
          LogWriterUtils.getLogWriter().info(
              "Got expected AuthenticationFailedException when doing puts: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
          LogWriterUtils.getLogWriter().info("Got expected exception when doing puts: " + ex);
        }
        else {
          Assert.fail("Got unexpected exception when doing puts", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
          LogWriterUtils.getLogWriter().info("Got expected exception when doing puts: " + ex);
        }
        else {
          Assert.fail("Got unexpected exception when doing puts", ex);
        }
      }
    }
  }

  public static void doGets(Integer num, Integer expectedResult,
      boolean newVals) {

    assertTrue(num.intValue() <= SecurityTestUtil.keys.length);
    Region region = null;
    try {
      region = SecurityTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
        LogWriterUtils.getLogWriter().info("Got expected exception when doing gets: " + ex);
      }
      else {
        Assert.fail("Got unexpected exception when doing gets", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      Object value = null;
      try {
        try {
          region.localInvalidate(SecurityTestUtil.keys[index]);
        }
        catch (Exception ex) {
        }
        value = region.get(SecurityTestUtil.keys[index]);
        if (expectedResult.intValue() != SecurityTestUtil.NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing gets");
        }
      }
      catch(NoAvailableServersException ex) {
        if(expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          LogWriterUtils.getLogWriter().info(
              "Got expected NoAvailableServers when doing puts: "
              + ex.getCause());
          continue;
        }
        else {
          Assert.fail("Got unexpected exception when doing puts", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == SecurityTestUtil.NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          LogWriterUtils.getLogWriter().info(
              "Got expected NotAuthorizedException when doing gets: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
          LogWriterUtils.getLogWriter().info("Got expected exception when doing gets: " + ex);
        }
        else {
          Assert.fail("Got unexpected exception when doing gets", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
          LogWriterUtils.getLogWriter().info("Got expected exception when doing gets: " + ex);
        }
        else {
          Assert.fail("Got unexpected exception when doing gets", ex);
        }
      }
      assertNotNull(value);
      assertEquals(deltas[index], value);
    }
  }

}
