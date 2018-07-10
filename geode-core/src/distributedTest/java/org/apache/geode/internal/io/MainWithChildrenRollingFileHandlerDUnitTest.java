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

package org.apache.geode.internal.io;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;

import java.net.InetAddress;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;


public class MainWithChildrenRollingFileHandlerDUnitTest {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Test
  public void testGeode2874_nameWithoutExtensionDoesNotThrowOnMemberRestart() throws Exception {
    MemberVM locatorVM = clusterStartupRule.startLocatorVM(0);

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.addPoolLocator(InetAddress.getLocalHost().toString(), locatorVM.getPort());
    clientCacheFactory.set(LOG_FILE, "nameWithoutExtension");
    ClientCache clientCache = clientCacheFactory.create();

    clientCache.close();
    ClientCache clientCache2 = clientCacheFactory.create();
  }
}
