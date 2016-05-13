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

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import org.junit.rules.ExternalResource;

public class ShiroCacheStartRule extends ExternalResource {
  private Cache cache;
  private int jmxManagerPort;
  private String shiroFile;

  public ShiroCacheStartRule(int jmxManagerPort, String shiroFile) {
    this.jmxManagerPort = jmxManagerPort;
    this.shiroFile = shiroFile;
  }


  protected void before() throws Throwable {
    Properties properties = new Properties();
    properties.put(DistributionConfig.NAME_NAME, ShiroCacheStartRule.class.getSimpleName());
    properties.put(DistributionConfig.LOCATORS_NAME, "");
    properties.put(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.put(DistributionConfig.JMX_MANAGER_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(jmxManagerPort));
    properties.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
    properties.put(DistributionConfig.SECURITY_SHIRO_INIT_NAME, shiroFile);

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
