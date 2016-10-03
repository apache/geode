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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public class AbstractSecureServerDUnitTest extends JUnit4CacheTestCase {

  protected static final String REGION_NAME = "AuthRegion";

  protected VM client1 = null;
  protected VM client2 = null;
  protected VM client3 = null;
  protected int serverPort;

  // child classes can customize these parameters
  protected Class postProcessor = null;
  protected boolean pdxPersistent = false;
  protected int jmxPort = 0;
  protected int restPort = 0;
  protected Map<String, Object> values;
  protected volatile Properties dsProperties;

  public AbstractSecureServerDUnitTest(){
    values = new HashMap();
    for(int i=0; i<5; i++){
      values.put("key"+i, "value"+i);
    }
  }

  @Before
  public void before() throws Exception {
    IgnoredException.addIgnoredException("No longer connected to localhost");

    final Host host = Host.getHost(0);
    this.client1 = host.getVM(1);
    this.client2 = host.getVM(2);
    this.client3 = host.getVM(3);

    Properties props = new Properties();
    props.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/clientServer.json");
    props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
//    props.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    if (postProcessor!=null) {
      props.setProperty(SECURITY_POST_PROCESSOR, postProcessor.getName());
    }
    props.setProperty(SECURITY_LOG_LEVEL, "finest");

    props.setProperty("security-pdx", pdxPersistent+"");
    if(jmxPort>0){
      props.put(JMX_MANAGER, "true");
      props.put(JMX_MANAGER_START, "true");
      props.put(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    }

    if(restPort>0){
      props.setProperty(START_DEV_REST_API, "true");
      props.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
      props.setProperty(HTTP_SERVICE_PORT, restPort+"");
    }

    props.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    
    this.dsProperties = props;

    getSystem(props);

    CacheFactory cf = new CacheFactory();
    cf.setPdxPersistent(pdxPersistent);
    cf.setPdxReadSerialized(pdxPersistent);
    Cache cache = getCache(cf);

    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);

    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();

    this.serverPort = server.getPort();

    for(Entry entry:values.entrySet()){
      region.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    return dsProperties;
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(()->closeCache());
    closeCache();
  }

  public static void assertNotAuthorized(ThrowingCallable shouldRaiseThrowable, String permString) {
    assertThatThrownBy(shouldRaiseThrowable).hasMessageContaining(permString);
  }

  public static Properties createClientProperties(String userName, String password) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    props.setProperty(SECURITY_LOG_LEVEL, "finest");
    return props;
  }

  public static ClientCache createClientCache(String username, String password, int serverPort){
    ClientCache cache = new ClientCacheFactory(createClientProperties(username, password))
      .setPoolSubscriptionEnabled(true)
      .addPoolServer("localhost", serverPort)
      .create();

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
    return cache;
  }

}
