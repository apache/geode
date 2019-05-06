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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ServerLauncherParameters;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class StartServerWithXmlDUnitTest {

  private static Cache cache;

  private VM server;
  private MemberVM locator;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0);

    String locators = "localhost[" + locator.getPort() + "]";
    String cacheXmlPath =
        createTempFileFromResource(getClass(), "CacheServerWithZeroPort.xml")
            .getAbsolutePath();

    Properties props = new Properties();
    props.setProperty(LOCATORS, locators);
    props.setProperty(CACHE_XML_FILE, cacheXmlPath);

    server = cluster.getVM(1);

    server.invoke(() -> {
      ServerLauncherParameters.INSTANCE.withBindAddress("localhost");
      cache = new CacheFactory(props).create();
    });
  }

  @After
  public void tearDown() {
    server.invoke(() -> {
      cache.close();
      cache = null;
    });
  }

  @Test
  public void startServerWithXMLNotToStartDefaultCacheServer() {
    // Verify that when there is a declarative cache server then we don't launch default server
    server.invoke(() -> {
      assertThat(cache.getCacheServers().size()).isEqualTo(1);
    });
  }
}
