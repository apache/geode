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

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.io.Serializable;
import java.util.Properties;

import org.junit.rules.ExternalResource;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;


/**
 * This is a rule to start up a server in your current VM. It's useful for your
 * Integration Tests.
 *
 * If you need a rule to start a server/locator in different VM for Distribution tests,
 * You should use LocatorServerStartupRule
 *
 * This rule does not have a before(), because you may choose to start a server in different time
 * of your tests. You may choose to use this class not as a rule or use it in your own rule,
 * (see LocatorServerStartupRule) you will need to call after() manually in that case.
 */
public class ServerStarter extends ExternalResource implements Serializable{

  public Cache cache;
  public CacheServer server;

  private  Properties properties;

  public ServerStarter(Properties properties){
    this.properties = properties;
  }

  public void startServer() throws Exception {
    startServer(0, false);
  }

  public void startServer(int locatorPort) throws Exception {
    startServer(locatorPort, false);
  }

  public void startServer(int locatorPort, boolean pdxPersistent) throws Exception {
    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }
    if (!properties.containsKey(NAME)) {
      properties.setProperty(NAME, this.getClass().getName());
    }
    if (locatorPort>0) {
      properties.setProperty(LOCATORS, "localhost["+locatorPort+"]");
    }
    else {
      properties.setProperty(LOCATORS, "");
    }
    if(properties.containsKey(JMX_MANAGER_PORT)){
      int jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      if(jmxPort>0) {
        if (!properties.containsKey(JMX_MANAGER))
          properties.put(JMX_MANAGER, "true");
        if (!properties.containsKey(JMX_MANAGER_START))
          properties.put(JMX_MANAGER_START, "true");
      }
    }

    CacheFactory cf = new CacheFactory(properties);
    cf.setPdxReadSerialized(pdxPersistent);
    cf.setPdxPersistent(pdxPersistent);

    cache = cf.create();
    server = cache.addCacheServer();
    server.setPort(0);
    server.start();
  }

  public void after(){
    if(cache!=null) cache.close();
    if(server!=null) server.stop();
  }
}
