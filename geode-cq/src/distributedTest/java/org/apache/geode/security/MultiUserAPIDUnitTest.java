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
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.io.IOException;
import java.util.Properties;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.Query;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class MultiUserAPIDUnitTest extends ClientAuthorizationTestCase {

  private static final String[] serverIgnoredExceptions =
      {AuthenticationRequiredException.class.getName(),
          AuthenticationFailedException.class.getName(), GemFireSecurityException.class.getName(),
          ClassNotFoundException.class.getName(), IOException.class.getName(),
          SSLException.class.getName(), SSLHandshakeException.class.getName()};

  private static final String[] clientIgnoredExceptions =
      {AuthenticationRequiredException.class.getName(),
          AuthenticationFailedException.class.getName(), SSLHandshakeException.class.getName()};

  @Test
  public void testSingleUserUnsupportedAPIs() {
    // Start servers
    // Start clients with multiuser-authentication set to false
    setUpVMs(new DummyCredentialGenerator(), false);
    client1.invoke(() -> verifyDisallowedOps(false));
  }

  @Test
  public void testMultiUserUnsupportedAPIs() {
    // Start servers.
    // Start clients with multiuser-authentication set to true.
    setUpVMs(new DummyCredentialGenerator(), true);
    client1.invoke(() -> verifyDisallowedOps(true));
  }

  private void verifyDisallowedOps(final boolean multiUserMode) throws Exception {
    String op = "unknown";
    boolean success = false;

    if (!multiUserMode) {
      success = false;

      try {
        // Attempt cache.createAuthenticatedCacheView() and expect an exception, fail otherwise
        op = "Pool.createSecureUserCache()";
        GemFireCacheImpl.getInstance().createAuthenticatedView(new Properties(), "testPool");
      } catch (IllegalStateException uoe) {
        getLogWriter().info(op + ": Got expected exception: " + uoe);
        success = true;
      }

      if (!success) {
        fail("Did not get exception while doing " + op);
      }

    } else { // multiuser mode
      Region realRegion = GemFireCacheImpl.getInstance().getRegion(SecurityTestUtils.REGION_NAME);
      Region proxyRegion =
          SecurityTestUtils.getProxyCaches(0).getRegion(SecurityTestUtils.REGION_NAME);
      Pool pool = PoolManagerImpl.getPMI().find("testPool");

      for (int i = 0; i <= 27; i++) {
        success = false;
        try {
          switch (i) {
            // Attempt (real) Region.create/put/get/containsKeyOnServer/destroy/
            // destroyRegion/clear/remove/registerInterest/unregisterInterest()
            // and expect an exception, fail otherwise.
            case 0:
              op = "Region.create()";
              realRegion.create("key", "value");
              break;
            case 1:
              op = "Region.put()";
              realRegion.put("key", "value");
              break;
            case 2:
              op = "Region.get()";
              realRegion.get("key");
              break;
            case 3:
              op = "Region.containsKeyOnServer()";
              realRegion.containsKeyOnServer("key");
              break;
            case 4:
              op = "Region.remove()";
              realRegion.remove("key");
              break;
            case 5:
              op = "Region.destroy()";
              realRegion.destroy("key");
              break;
            case 6:
              op = "Region.destroyRegion()";
              realRegion.destroyRegion();
              break;
            case 7:
              op = "Region.registerInterest()";
              realRegion.registerInterest("key");
              break;
            // case 8:
            // op = "Region.unregisterInterest()";
            // realRegion.unregisterInterest("key");
            // break;
            case 8:
              op = "Region.clear()";
              realRegion.clear();
              break;
            // Attempt ProxyRegion.createSubregion/forceRolling/
            // getAttributesMutator/registerInterest/loadSnapShot/saveSnapshot/
            // setUserAttribute/unregisterInterest/writeToDisk
            // and expect an exception, fail otherwise.
            case 9:
              op = "ProxyRegion.createSubregion()";
              proxyRegion.createSubregion("subregion", null);
              break;
            case 10:
              op = "ProxyRegion.forceRolling()";
              proxyRegion.forceRolling();
              break;
            case 11:
              op = "ProxyRegion.getAttributesMutator()";
              proxyRegion.getAttributesMutator();
              break;
            case 12:
              op = "ProxyRegion.registerInterest()";
              proxyRegion.registerInterest("key");
              break;
            case 13:
              op = "ProxyRegion.loadSnapshot()";
              proxyRegion.loadSnapshot(null);
              break;
            case 14:
              op = "ProxyRegion.saveSnapshot()";
              proxyRegion.saveSnapshot(null);
              break;
            case 15:
              op = "ProxyRegion.setUserAttribute()";
              proxyRegion.setUserAttribute(null);
              break;
            case 16:
              op = "ProxyRegion.unregisterInterestRegex()";
              proxyRegion.unregisterInterestRegex("*");
              break;
            // Attempt FunctionService.onRegion/onServer/s(pool) and expect an
            // exception, fail otherwise.
            case 17:
              op = "FunctionService.onRegion()";
              FunctionService.onRegion(realRegion);
              break;
            case 18:
              op = "FunctionService.onServer(pool)";
              FunctionService.onServer(pool);
              break;
            case 19:
              op = "FunctionService.onServers(pool)";
              FunctionService.onServers(pool);
              break;
            // Attempt
            // QueryService.newQuery().execute()/newCq().execute/executeWithInitialResults()
            case 20:
              op = "QueryService.newQuery.execute()";
              Query query = pool.getQueryService()
                  .newQuery("SELECT * FROM /" + SecurityTestUtils.REGION_NAME);
              query.execute();
              break;
            case 21:
              op = "QueryService.newCq.execute()";
              CqQuery cqQuery =
                  pool.getQueryService().newCq("SELECT * FROM /" + SecurityTestUtils.REGION_NAME,
                      new CqAttributesFactory().create());
              try {
                cqQuery.execute();
              } catch (CqException ce) {
                throw (Exception) ce.getCause();
              }
              break;
            case 22:
              op = "QueryService.newCq.executeWithInitialResults()";
              cqQuery =
                  pool.getQueryService().newCq("SELECT * FROM /" + SecurityTestUtils.REGION_NAME,
                      new CqAttributesFactory().create());
              try {
                cqQuery.executeWithInitialResults();
              } catch (CqException ce) {
                throw (Exception) ce.getCause();
              }
              break;
            // Attempt ProxyQueryService.getIndex/createIndex/removeIndex() and
            // expect an exception, fail otherwise.
            case 23:
              op = "ProxyQueryService().getIndexes()";
              SecurityTestUtils.getProxyCaches(0).getQueryService().getIndexes(null);
              break;
            case 24:
              op = "ProxyQueryService().createIndex()";
              SecurityTestUtils.getProxyCaches(0).getQueryService().createIndex(null, null, null);
              break;
            case 25:
              op = "ProxyQueryService().removeIndexes()";
              SecurityTestUtils.getProxyCaches(0).getQueryService().removeIndexes();
              break;
            case 26:
              op = "ProxyRegion.localDestroy()";
              proxyRegion.localDestroy("key");
              break;
            case 27:
              op = "ProxyRegion.localInvalidate()";
              proxyRegion.localInvalidate("key");
              break;
            default:
              fail("Unknown op code: " + i);
              break;
          }

        } catch (UnsupportedOperationException uoe) {
          getLogWriter().info(op + ": Got expected exception: " + uoe);
          success = true;
        }
        if (!success) {
          fail("Did not get exception while doing " + op);
        }
      }
    }
  }

  private void setUpVMs(final CredentialGenerator gen, final boolean multiUser) {
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testValidCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info("testValidCredentials: Using authenticator: " + authenticator);
    getLogWriter().info("testValidCredentials: Using authinit: " + authInit);

    // Start the servers
    int port1 = server1
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));
    int port2 = server2
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Start the clients with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info(
        "testValidCredentials: For first client credentials: " + credentials1 + " : " + javaProps1);

    Properties credentials2 = gen.getValidCredentials(2);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testValidCredentials: For second client credentials: " + credentials2
        + " : " + javaProps2);

    client1.invoke(() -> createCacheClient(authInit, credentials1, javaProps1, port1, port2, 0,
        multiUser, NO_EXCEPTION));
  }

  private int createCacheServer(final String authenticator, final Properties extraProps,
      final Properties javaProps) {
    Properties authProps = new Properties();
    if (extraProps != null) {
      authProps.putAll(extraProps);
    }

    if (authenticator != null) {
      authProps.setProperty(ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR, authenticator);
    }

    return SecurityTestUtils.createCacheServer(authProps, javaProps, 0, NO_EXCEPTION);
  }

  // a
  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int[] ports, final int numConnections,
      final boolean multiUserMode, final int expectedResult) {
    SecurityTestUtils.createCacheClient(authInit, authProps, javaProps, ports, numConnections,
        multiUserMode, expectedResult); // invokes SecurityTestUtils 2
  }

  // b
  private void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int port1, final int port2, final int numConnections,
      final boolean multiUserMode, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, new int[] {port1, port2}, numConnections,
        multiUserMode, expectedResult); // invokes a
  }
}
