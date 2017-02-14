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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PDXPostProcessorDUnitTest extends AbstractSecureServerDUnitTest {
  private static byte[] BYTES = PDXPostProcessor.BYTES;
  private int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    Object[][] params = {{true}, {false}};
    return Arrays.asList(params);
  }

  public Properties getProperties() {
    Properties properties = super.getProperties();
    properties.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    properties.setProperty(JMX_MANAGER_PORT, jmxPort + "");
    properties.setProperty("security-pdx", pdxPersistent + "");
    return properties;
  }

  public Map<String, String> getData() {
    return new HashMap();
  }

  public PDXPostProcessorDUnitTest(boolean pdxPersistent) {
    this.pdxPersistent = pdxPersistent;
  }

  @Test
  public void testRegionGet() {
    client2.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      // put in a byte value
      region.put("key2", BYTES);
    });

    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

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
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

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

  @Category(FlakyTest.class) // GEODE-2204
  @Test
  public void testRegisterInterest() {
    client1.invoke(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("super-user", "1234567"))
          .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort).create();

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
      ClientCache cache = createClientCache("dataUser", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
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

  @Category(FlakyTest.class) // GEODE-1719
  @Test
  public void testGfshCommand() {
    // have client2 input some domain data into the region
    client2.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      // put in a byte value
      region.put("key2", BYTES);
    });

    client1.invoke(() -> {
      CliUtil.isGfshVM = true;
      String shellId = getClass().getSimpleName();
      HeadlessGfsh gfsh = new HeadlessGfsh(shellId, 30, "gfsh_files");

      // connect to the jmx server
      final CommandStringBuilder connectCommand = new CommandStringBuilder(CliStrings.CONNECT);
      connectCommand.addOption(CliStrings.CONNECT__USERNAME, "dataUser");
      connectCommand.addOption(CliStrings.CONNECT__PASSWORD, "1234567");

      String endpoint = "localhost[" + jmxPort + "]";
      connectCommand.addOption(CliStrings.CONNECT__JMX_MANAGER, endpoint);

      gfsh.executeCommand(connectCommand.toString());
      CommandResult result = (CommandResult) gfsh.getResult();

      // get command
      gfsh.executeCommand("get --key=key1 --region=AuthRegion");
      result = (CommandResult) gfsh.getResult();
      assertEquals(result.getStatus(), Status.OK);
      if (pdxPersistent)
        assertTrue(result.getContent().toString()
            .contains("org.apache.geode.pdx.internal.PdxInstanceImpl"));
      else
        assertTrue(result.getContent().toString().contains("SimpleClass"));

      gfsh.executeCommand("get --key=key2 --region=AuthRegion");
      result = (CommandResult) gfsh.getResult();
      assertEquals(result.getStatus(), Status.OK);
      assertTrue(result.getContent().toString().contains("byte[]"));

      gfsh.executeCommand("query --query=\"select * from /AuthRegion\"");
      result = (CommandResult) gfsh.getResult();
      System.out.println("gfsh result: " + result);
    });

    PDXPostProcessor pp =
        (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 4);
  }

}
