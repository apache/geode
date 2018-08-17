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

import static org.apache.geode.security.ClientAuthenticationTestUtils.createCacheClient;
import static org.apache.geode.security.SecurityTestUtils.KEYS;
import static org.apache.geode.security.SecurityTestUtils.NO_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.REGION_NAME;
import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.security.SecurityTestUtils.getCache;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DeltaTestImpl;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.internal.cache.TestObjectWithIdentifier;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * @since GemFire 6.1
 */
@Category({SecurityTest.class})
public class DeltaClientAuthorizationDUnitTest extends ClientAuthorizationTestCase {

  private DeltaTestImpl[] deltas = new DeltaTestImpl[8];

  @Override
  protected final void preSetUpClientAuthorizationTestBase() throws Exception {
    setUpDeltas();
  }

  @Override
  public final void preTearDownClientAuthorizationTestBase() throws Exception {
    closeCache();
  }

  @Test
  public void testAllowPutsGets() throws Exception {
    AuthzCredentialGenerator gen = this.getXmlAuthzGenerator();
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
    Properties serverProps =
        buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    int port1 = createServer1(javaProps, serverProps);
    int port2 = createServer2(javaProps, serverProps);

    // Start client1 with valid CREATE credentials
    Properties createCredentials = gen.getAllowedCredentials(
        new OperationCode[] {OperationCode.PUT}, new String[] {REGION_NAME}, 1);
    javaProps = cGen.getJavaProperties();

    getLogWriter().info("testAllowPutsGets: For first client credentials: " + createCredentials);

    createClient1(javaProps, authInit, port1, port2, createCredentials);

    // Start client2 with valid GET credentials
    Properties getCredentials = gen.getAllowedCredentials(new OperationCode[] {OperationCode.GET},
        new String[] {REGION_NAME}, 2);
    javaProps = cGen.getJavaProperties();

    getLogWriter().info("testAllowPutsGets: For second client credentials: " + getCredentials);

    createClient2(javaProps, authInit, port1, port2, getCredentials);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2, NO_EXCEPTION));

    Thread.sleep(5000);
    assertTrue("Delta feature NOT used", client1.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed()));

    // Verify that the gets succeed
    client2.invoke(() -> doGets(2, NO_EXCEPTION));
  }

  private void createClient2(final Properties javaProps, final String authInit, final int port1,
      final int port2, final Properties getCredentials) {
    client2.invoke(() -> createCacheClient(authInit, getCredentials, javaProps, port1, port2, 0,
        NO_EXCEPTION));
  }

  private void createClient1(final Properties javaProps, final String authInit, final int port1,
      final int port2, final Properties createCredentials) {
    client1.invoke(() -> createCacheClient(authInit, createCredentials, javaProps, port1, port2, 0,
        NO_EXCEPTION));
  }

  private int createServer2(final Properties javaProps, final Properties serverProps) {
    return server2.invoke(() -> createCacheServer(serverProps, javaProps));
  }

  private int createServer1(final Properties javaProps, final Properties serverProps) {
    return server1.invoke(() -> createCacheServer(serverProps, javaProps));
  }

  private void doPuts(final int num, final int expectedResult) {
    assertTrue(num <= KEYS.length);
    Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);
    for (int index = 0; index < num; ++index) {
      region.put(KEYS[index], deltas[0]);
    }
    for (int index = 0; index < num; ++index) {
      region.put(KEYS[index], deltas[index]);
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while doing puts");
      }
    }
  }

  private void doGets(final int num, final int expectedResult) {
    assertTrue(num <= KEYS.length);

    Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);

    for (int index = 0; index < num; ++index) {
      region.localInvalidate(KEYS[index]);
      Object value = region.get(KEYS[index]);
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while doing gets");
      }
      assertNotNull(value);
      assertEquals(deltas[index], value);
    }
  }

  private void setUpDeltas() {
    for (int i = 0; i < 8; i++) {
      deltas[i] = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObjectWithIdentifier("0", 0));
    }
    deltas[1].setIntVar(5);
    deltas[2].setIntVar(5);
    deltas[3].setIntVar(5);
    deltas[4].setIntVar(5);
    deltas[5].setIntVar(5);
    deltas[6].setIntVar(5);
    deltas[7].setIntVar(5);

    deltas[2].resetDeltaStatus();
    deltas[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltas[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltas[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltas[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

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

    deltas[5].resetDeltaStatus();
    deltas[5].setIntVar(100);
    deltas[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
    deltas[6].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
    deltas[7].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));

    deltas[6].resetDeltaStatus();
    deltas[6].setByteArr(new byte[] {1, 2, 3});
    deltas[7].setByteArr(new byte[] {1, 2, 3});

    deltas[7].resetDeltaStatus();
    deltas[7].setStr("delta string");
  }
}
