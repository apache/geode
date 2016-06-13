/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import org.junit.rules.ExternalResource;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

public class JsonAuthorizationCacheStartRule extends ExternalResource {
  private Cache cache;
  private int jmxManagerPort = 0;
  private int httpPort = 0;
  private String jsonFile;
  private boolean doAuthorization;

  public JsonAuthorizationCacheStartRule(int jmxManagerPort, String jsonFile) {
    this.jmxManagerPort = jmxManagerPort;
    this.jsonFile = jsonFile;
    this.doAuthorization = true;
  }

  public JsonAuthorizationCacheStartRule(int jmxManagerPort, int httpPort, String jsonFile) {
    this.jmxManagerPort = jmxManagerPort;
    this.httpPort = httpPort;
    this.jsonFile = jsonFile;
    this.doAuthorization = true;
  }

  public JsonAuthorizationCacheStartRule(int jmxManagerPort, String jsonFile, boolean doAuthorization) {
    this.jmxManagerPort = jmxManagerPort;
    this.jsonFile = jsonFile;
    this.doAuthorization = doAuthorization;
  }

  protected void before() throws Throwable {
    Properties properties = new Properties();
    properties.put(NAME, JsonAuthorizationCacheStartRule.class.getSimpleName());
    properties.put(LOCATORS, "");
    properties.put(MCAST_PORT, "0");
    properties.put(JMX_MANAGER, "true");
    properties.put(JMX_MANAGER_START, "true");
    properties.put(JMX_MANAGER_PORT, String.valueOf(jmxManagerPort));
    properties.put(HTTP_SERVICE_PORT, String.valueOf(httpPort));
    properties.put(SECURITY_CLIENT_AUTHENTICATOR,
        JSONAuthorization.class.getName() + ".create");
    if (doAuthorization) {
      properties.put(SECURITY_CLIENT_ACCESSOR, JSONAuthorization.class.getName() + ".create");
    }
    JSONAuthorization.setUpWithJsonFile(jsonFile);

    cache = new CacheFactory(properties).create();
    cache.addCacheServer().start();
  }

  public Cache getCache(){
    return cache;
  }

  /**
   * Override to tear down your specific external resource.
   */
  protected void after() {
    cache.close();
    cache = null;
  }
}
