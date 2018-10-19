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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR_PP;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.dunit.CloseCacheAuthorization;
import org.apache.geode.cache.query.dunit.HelperTestCase;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class CqStateDUnitTest extends HelperTestCase {

  // this test is disabled due to a 25% failure rate in
  // CI testing. See internal ticket #52229
  @Ignore("TODO: test is disabled due to flickering")
  @Test
  public void testBug51222() throws Exception {
    // The client can log this when the server shuts down.
    IgnoredException.addIgnoredException("Could not find any server");
    IgnoredException.addIgnoredException("java.net.ConnectException");
    final String cqName = "theCqInQuestion";
    final String regionName = "aattbbss";
    final Host host = Host.getHost(0);
    VM serverA = host.getVM(1);
    VM serverB = host.getVM(2);
    VM client = host.getVM(3);

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    startCacheServer(serverA, ports[0], getAuthenticatedServerProperties());
    createReplicatedRegion(serverA, regionName, null);

    final String host0 = NetworkUtils.getServerHostName(serverA.getHost());
    startClient(client, new VM[] {serverA, serverB}, ports, 1, getClientProperties());
    createCQ(client, cqName, "select * from /" + regionName, null);

    // create the cacheserver but regions must be present first or else cq execute will fail with no
    // region found
    createCacheServer(serverB, ports[1], getServerProperties(0));
    createReplicatedRegion(serverB, regionName, null);
    startCacheServers(serverB);

    AsyncInvocation async = executeCQ(client, cqName);
    ThreadUtils.join(async, 10000);

    client.invoke(() -> {
      final CqQuery cq = getCache().getQueryService().getCq(cqName);
      await("Waiting for CQ to be in running state: " + cq).until(() -> cq.getState().isRunning());
    });

    // hope that server 2 comes up before num retries is exhausted by the execute cq command
    // hope that the redundancy satisfier sends message and is executed after execute cq has been
    // executed
    // This is the only way bug 51222 would be noticed
    // verify that the cq on the server is still in RUNNING state;
    Boolean isRunning = (Boolean) serverB.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CqQuery cq = getCache().getQueryService().getCqs()[0];
        return cq.getState().isRunning();
      }
    });

    assertTrue("Cq was not running on server", isRunning);
  }

  public Properties getAuthenticatedServerProperties() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(SECURITY_CLIENT_ACCESSOR, CloseCacheAuthorization.class.getName() + ".create");
    props.put(SECURITY_CLIENT_ACCESSOR_PP, CloseCacheAuthorization.class.getName() + ".create");
    props.put(SECURITY_CLIENT_AUTHENTICATOR, DummyAuthenticator.class.getName() + ".create");
    return props;
  }

  public Properties getServerProperties() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    return props;
  }

  public Properties getClientProperties() {
    Properties props = new Properties();
    props.put(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    props.put("security-username", "root");
    props.put("security-password", "root");
    return props;
  }

}
