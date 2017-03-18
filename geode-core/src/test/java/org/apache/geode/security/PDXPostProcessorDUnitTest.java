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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PDXPostProcessorDUnitTest extends JUnit4DistributedTestCase {
  private static String REGION_NAME = "AuthRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  private boolean pdxPersistent = false;
  private static byte[] BYTES = PDXPostProcessor.BYTES;

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    Object[][] params = {{true}, {false}};
    return Arrays.asList(params);
  }

  public PDXPostProcessorDUnitTest(boolean pdxPersistent) {
    this.pdxPersistent = pdxPersistent;
  }

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName())
          .withProperty("security-pdx", pdxPersistent + "").withJMXManager().startServer()
          .createRegion(RegionShortcut.REPLICATE, REGION_NAME);

  @Test
  public void testRegionGet() {
    client2.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      // put in a byte value
      region.put("key2", BYTES);
    });

    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

      // post process for get the client domain object
      Object value = region.get("key1");
      assertTrue(value instanceof SimpleClass);

      // post process for get the raw byte value
      value = region.get("key2");
      assertTrue(Arrays.equals(BYTES, (byte[]) value));
    });

    // this makes sure PostProcessor is getting called
    PDXPostProcessor pp =
        (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

  @Test
  public void testQuery() {
    client2.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

      // post process for query
      String query = "select * from /AuthRegion";
      SelectResults result = region.query(query);

      Iterator itr = result.iterator();
      while (itr.hasNext()) {
        Object obj = itr.next();
        if (obj instanceof byte[]) {
          assertTrue(Arrays.equals(BYTES, (byte[]) obj));
        } else {
          assertTrue(obj instanceof SimpleClass);
        }
      }
    });

    // this makes sure PostProcessor is getting called
    PDXPostProcessor pp =
        (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

  @Test
  public void testRegisterInterest() {
    IgnoredException.addIgnoredException("NoAvailableServersException");
    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());

      ClientRegionFactory factory = cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      factory.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterUpdate(EntryEvent event) {
          Object key = event.getKey();
          Object value = ((EntryEventImpl) event).getDeserializedValue();
          if (key.equals("key1")) {
            assertTrue(value instanceof SimpleClass);
          } else if (key.equals("key2")) {
            assertTrue(Arrays.equals(BYTES, (byte[]) value));
          }
        }
      });

      Region region = factory.create(REGION_NAME);
      region.put("key1", "value1");
      region.registerInterest("key1");
      region.registerInterest("key2");
    });

    client2.invoke(() -> {
      ClientCache cache = createClientCache("dataUser", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    // wait for events to fire
    Awaitility.await().atMost(1, TimeUnit.SECONDS);
    PDXPostProcessor pp =
        (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

  @Test
  public void testGfshCommand() {
    // have client2 input some domain data into the region
    client2.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      // put in a byte value
      region.put("key2", BYTES);
    });

    client1.invoke(() -> {
      GfshShellConnectionRule gfsh = new GfshShellConnectionRule();
      gfsh.secureConnectAndVerify(server.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger,
          "dataUser", "1234567");

      // get command
      CommandResult result = gfsh.executeAndVerifyCommand("get --key=key1 --region=AuthRegion");
      if (pdxPersistent)
        assertThat(gfsh.getGfshOutput().contains("org.apache.geode.pdx.internal.PdxInstanceImpl"));
      else
        assertThat(gfsh.getGfshOutput()).contains("SimpleClass");

      result = gfsh.executeAndVerifyCommand("get --key=key2 --region=AuthRegion");
      assertTrue(result.getContent().toString().contains("byte[]"));

      gfsh.executeAndVerifyCommand("query --query=\"select * from /AuthRegion\"");
      gfsh.close();
    });

    PDXPostProcessor pp =
        (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 4);
  }

}
