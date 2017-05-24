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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.assertFalse;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Category({DistributedTest.class, ClientServerTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientServerMiscBCDUnitTest extends ClientServerMiscDUnitTest {
  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  public ClientServerMiscBCDUnitTest(String version) {
    super();
    testVersion = version;
  }

  @Test
  public void testSubscriptionWithCurrentServerAndOldClients() throws Exception {
    // start server first
    int serverPort = initServerCache(true);
    VM client1 = Host.getHost(0).getVM(testVersion, 1);
    VM client2 = Host.getHost(0).getVM(testVersion, 3);
    String hostname = NetworkUtils.getServerHostName(Host.getHost(0));
    client1.invoke("create client1 cache", () -> {
      createClientCache(hostname, serverPort);
      populateCache();
      registerInterest();
    });
    client2.invoke("create client2 cache", () -> {
      Pool ignore = createClientCache(hostname, serverPort);
    });

    client2.invoke("putting data in client2", () -> putForClient());

    // client1 will receive client2's updates asynchronously
    client1.invoke(() -> {
      Region r2 = getCache().getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> verifier.eventReceived);
    });

    // client2's update should have included a memberID - GEODE-2954
    client1.invoke(() -> {
      Region r2 = getCache().getRegion(REGION_NAME2);
      MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
      assertFalse(verifier.memberIDNotReceived);
    });
  }

}
