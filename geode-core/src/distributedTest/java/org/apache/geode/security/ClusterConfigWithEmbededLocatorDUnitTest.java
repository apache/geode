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

package org.apache.geode.security;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class ClusterConfigWithEmbededLocatorDUnitTest extends JUnit4DistributedTestCase {
  protected VM locator = null;

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    this.locator = host.getVM(0).initializeAsLocatorVM();
  }

  @Test
  public void testEmbeddedLocator() throws Exception {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    // locator started this way won't have cluster configuration running
    locator.invoke(() -> {
      new CacheFactory().set("name", this.getName() + ".server1").set("mcast-port", "0")
          .set("log-level", "config").set("start-locator", "localhost[" + locatorPort + "]")
          .create();
    });

    // when this server joins the above locator, it won't request the cluster config from the
    // locator
    // since DM.getAllHostedLocatorsWithSharedConfiguration() will return an empty list. This would
    // execute without error

    new CacheFactory().set("name", this.getName() + ".server2").set("mcast-port", "0")
        .set("log-level", "config").set("locators", "localhost[" + locatorPort + "]").create();
  }
}
