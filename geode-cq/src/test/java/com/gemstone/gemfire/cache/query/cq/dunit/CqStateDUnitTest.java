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
package com.gemstone.gemfire.cache.query.cq.dunit;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.dunit.CloseCacheAuthorization;
import com.gemstone.gemfire.cache.query.dunit.HelperTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.security.templates.DummyAuthenticator;
import com.gemstone.gemfire.security.templates.UserPasswordAuthInit;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class CqStateDUnitTest extends HelperTestCase {

  // this test is disabled due to a 25% failure rate in
  // CI testing.  See internal ticket #52229
  @Ignore("TODO: test is disabled due to flickering")
  @Test
  public void testBug51222() throws Exception {
    //The client can log this when the server shuts down.
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
    startClient(client, new VM[]{ serverA, serverB }, ports, 1, getClientProperties());
    createCQ(client, cqName, "select * from /"+ regionName, null);
    
    //create the cacheserver but regions must be present first or else cq execute will fail with no region found
    createCacheServer(serverB, ports[1], getServerProperties(0));
    createReplicatedRegion(serverB, regionName, null);
    startCacheServers(serverB);
    
    AsyncInvocation async = executeCQ(client, cqName);
    ThreadUtils.join(async, 10000);

    Boolean clientRunning = (Boolean) client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final CqQuery cq = getCache().getQueryService().getCq(cqName);
        Wait.waitForCriterion(new WaitCriterion() {
          @Override
          public boolean done() {
            return cq.getState().isRunning();
          }
          @Override
          public String description() {
            return "waiting for Cq to be in a running state: " + cq;
          }
        },  30000, 1000, false);
        return cq.getState().isRunning();
      }
    });
    assertTrue("Client was not running", clientRunning);
    
    //hope that server 2 comes up before num retries is exhausted by the execute cq command
    //hope that the redundancy satisfier sends message and is executed after execute cq has been executed
    //This is the only way bug 51222 would be noticed
    //verify that the cq on the server is still in RUNNING state;
    Boolean isRunning = (Boolean) serverB.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CqQuery cq = getCache().getQueryService().getCqs()[0];
        return cq.getState().isRunning();
      }
    });
    
    assertTrue("Cq was not running on server" , isRunning);
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

