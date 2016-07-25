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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.geode.security.templates.SampleSecurityManager;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.security.templates.UserPasswordAuthInit;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;

public class AbstractIntegratedClientAuthDistributedTest extends JUnit4CacheTestCase {

  protected static final String REGION_NAME = "AuthRegion";

  protected VM client1 = null;
  protected VM client2 = null;
  protected VM client3 = null;
  protected int serverPort;
  protected Class postProcessor = null;

  @Before
  public void before() throws Exception{
    final Host host = Host.getHost(0);
    this.client1 = host.getVM(1);
    this.client2 = host.getVM(2);
    this.client3 = host.getVM(3);

    Properties props = new Properties();
    props.setProperty(SampleSecurityManager.SECURITY_JSON, "com/gemstone/gemfire/management/internal/security/clientServer.json");
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    if (postProcessor!=null) {
      props.setProperty(SECURITY_POST_PROCESSOR, postProcessor.getName());
    }
    props.setProperty(SECURITY_LOG_LEVEL, "finest");
    props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());

    getSystem(props);

    Cache cache = getCache();

    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);

    CacheServer server1 = cache.addCacheServer();
    server1.setPort(0);
    server1.start();

    this.serverPort = server1.getPort();

    for (int i = 0; i < 5; i++) {
      String key = "key" + i;
      String value = "value" + i;
      region.put(key, value);
    }
    assertEquals(5, region.size());
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(()->closeCache());
    closeCache();
  }

  public static void assertNotAuthorized(ThrowingCallable shouldRaiseThrowable, String permString) {
    assertThatThrownBy(shouldRaiseThrowable).hasMessageContaining(permString);
  }

  protected Properties createClientProperties(String userName, String password) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    props.setProperty(SECURITY_LOG_LEVEL, "finest");
    return props;
  }

  protected ClientCache createClientCache(String username, String password, int serverPort){
    ClientCache cache = new ClientCacheFactory(createClientProperties(username, password))
      .setPoolSubscriptionEnabled(true)
      .addPoolServer("localhost", serverPort)
      .create();

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
    return cache;
  }

}
