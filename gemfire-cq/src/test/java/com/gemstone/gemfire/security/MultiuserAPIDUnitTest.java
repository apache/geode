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

import hydra.Log;

import java.io.IOException;
import java.util.Properties;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import security.CredentialGenerator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

import security.DummyCredentialGenerator;

public class MultiuserAPIDUnitTest extends ClientAuthorizationTestBase {

  /** constructor */
  public MultiuserAPIDUnitTest(String name) {
    super(name);
  }

  private VM server1 = null;

  private VM server2 = null;

  private VM client1 = null;

  private VM client2 = null;

  private static final String[] serverExpectedExceptions = {
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      GemFireSecurityException.class.getName(),
      ClassNotFoundException.class.getName(), IOException.class.getName(),
      SSLException.class.getName(), SSLHandshakeException.class.getName()};

  private static final String[] clientExpectedExceptions = {
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      SSLHandshakeException.class.getName()};

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);

    server1.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] {serverExpectedExceptions});
    server2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] {serverExpectedExceptions});
    client1.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] {clientExpectedExceptions});
    client2.invoke(SecurityTestUtil.class, "registerExpectedExceptions",
        new Object[] {clientExpectedExceptions});
  }

  public static Integer createCacheServer(Object dsPort, Object locatorString,
      Object authenticator, Object extraProps, Object javaProps) {

    Properties authProps;
    if (extraProps == null) {
      authProps = new Properties();
    } else {
      authProps = (Properties)extraProps;
    }
    if (authenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, authenticator
              .toString());
    }
    return SecurityTestUtil.createCacheServer(authProps, javaProps,
        (Integer)dsPort, (String)locatorString, null, new Integer(
            SecurityTestUtil.NO_EXCEPTION));
  }

  private static void createCacheClient(Object authInit, Properties authProps,
      Properties javaProps, Integer[] ports, Object numConnections,
      Boolean multiUserMode, Integer expectedResult) {

    String authInitStr = (authInit == null ? null : authInit.toString());
    SecurityTestUtil.createCacheClient(authInitStr, authProps, javaProps,
        ports, numConnections, multiUserMode.toString(), expectedResult);
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer port1, Integer port2, Object numConnections,
      Boolean multiUserMode, Integer expectedResult) {

    createCacheClient(authInit, (Properties)authProps, (Properties)javaProps,
        new Integer[] {port1, port2}, numConnections, multiUserMode,
        expectedResult);
  }

  public static void registerAllInterest() {
    Region region = SecurityTestUtil.getCache().getRegion(
        SecurityTestUtil.regionName);
    assertNotNull(region);
    region.registerInterestRegex(".*");
  }

  private void setUpVMs(CredentialGenerator gen, Boolean multiUser) {
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info(
        "testValidCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info(
        "testValidCredentials: Using authenticator: " + authenticator);
    getLogWriter().info("testValidCredentials: Using authinit: " + authInit);

    // Start the servers
    Integer locPort1 = SecurityTestUtil.getLocatorPort();
    Integer locPort2 = SecurityTestUtil.getLocatorPort();
    String locString = SecurityTestUtil.getLocatorString();
    Integer port1 = (Integer)server1.invoke(MultiuserAPIDUnitTest.class,
        "createCacheServer", new Object[] {locPort1, locString, authenticator,
            extraProps, javaProps});
    Integer port2 = (Integer)server2.invoke(MultiuserAPIDUnitTest.class,
        "createCacheServer", new Object[] {locPort2, locString, authenticator,
            extraProps, javaProps});

    // Start the clients with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info(
        "testValidCredentials: For first client credentials: " + credentials1
            + " : " + javaProps1);
    Properties credentials2 = gen.getValidCredentials(2);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info(
        "testValidCredentials: For second client credentials: " + credentials2
            + " : " + javaProps2);
    client1.invoke(MultiuserAPIDUnitTest.class, "createCacheClient",
        new Object[] {authInit, credentials1, javaProps1, port1, port2, null,
            multiUser, new Integer(SecurityTestUtil.NO_EXCEPTION)});
  }

  public void testSingleUserUnsupportedAPIs() {
      // Start servers
      // Start clients with multiuser-authentication set to false
      setUpVMs(new DummyCredentialGenerator(), Boolean.FALSE);
      client1.invoke(MultiuserAPIDUnitTest.class, "verifyDisallowedOps",
          new Object[] {Boolean.FALSE});
  }

  public void testMultiUserUnsupportedAPIs() {
      // Start servers.
      // Start clients with multiuser-authentication set to true.
      setUpVMs(new DummyCredentialGenerator(), Boolean.TRUE);
      client1.invoke(MultiuserAPIDUnitTest.class, "verifyDisallowedOps",
          new Object[] {Boolean.TRUE});
  }

  public static void verifyDisallowedOps(Boolean multiuserMode) {
    String op = "unknown";
    boolean success = false;
    if (!multiuserMode) {
      success = false;
      try {
        // Attempt cache.createAuthenticatedCacheView() and expect an exception, fail otherwise
        op = "Pool.createSecureUserCache()";
        GemFireCacheImpl.getInstance().createAuthenticatedView(new Properties(), "testPool");
      } catch (IllegalStateException uoe) {
        Log.getLogWriter().info(op + ": Got expected exception: " + uoe);
        success = true;
      } catch (Exception e) {
        fail("Got unexpected exception while doing " + op, e);
      }
      if (!success) {
        fail("Did not get exception while doing " + op);
      }
    } else { // multiuser mode
      Region realRegion = GemFireCacheImpl.getInstance().getRegion(
          SecurityTestUtil.regionName);
      Region proxyRegion = SecurityTestUtil.proxyCaches[0]
          .getRegion(SecurityTestUtil.regionName);
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
              proxyRegion.createSubregion("subregion",
                  null);
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
              Query query = pool.getQueryService().newQuery(
                  "SELECT * FROM /" + SecurityTestUtil.regionName);
              query.execute();
              break;
            case 21:
              op = "QueryService.newCq.execute()";
              CqQuery cqQuery = pool.getQueryService().newCq(
                  "SELECT * FROM /" + SecurityTestUtil.regionName,
                  new CqAttributesFactory().create());
              try {
                cqQuery.execute();
              } catch (CqException ce) {
                throw (Exception)ce.getCause();
              }
              break;
            case 22:
              op = "QueryService.newCq.executeWithInitialResults()";
              cqQuery = pool.getQueryService().newCq(
                  "SELECT * FROM /" + SecurityTestUtil.regionName,
                  new CqAttributesFactory().create());
              try {
                cqQuery.executeWithInitialResults();
              } catch (CqException ce) {
                throw (Exception)ce.getCause();
              }
              break;
            // Attempt ProxyQueryService.getIndex/createIndex/removeIndex() and
            // expect an exception, fail otherwise.
            case 23:
              op = "ProxyQueryService().getIndexes()";
              SecurityTestUtil.proxyCaches[0].getQueryService()
                  .getIndexes(null);
              break;
            case 24:
              op = "ProxyQueryService().createIndex()";
              SecurityTestUtil.proxyCaches[0].getQueryService().createIndex(
                  null, null, null );
              break;
            case 25:
              op = "ProxyQueryService().removeIndexes()";
              SecurityTestUtil.proxyCaches[0].getQueryService().removeIndexes();
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
          Log.getLogWriter().info(op + ": Got expected exception: " + uoe);
          success = true;
        } catch (Exception e) {
          fail("Got unexpected exception while doing " + op, e);
        }
        if (!success) {
          fail("Did not get exception while doing " + op);
        }
      }
    }
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    // close the clients first
    client1.invoke(SecurityTestUtil.class, "closeCache");
    client2.invoke(SecurityTestUtil.class, "closeCache");
    // then close the servers
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");
  }

}
