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

package com.gemstone.gemfire.security;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.pdx.SimpleClass;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ DistributedTest.class, SecurityTest.class })
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PDXPostProcessorDUnitTest extends AbstractSecureServerDUnitTest {
  private static byte[] BYTES = PDXPostProcessor.BYTES;

  @Parameterized.Parameters
  public static Collection<Object[]> parameters(){
    Object[][] params = {{true}, {false}};
    return Arrays.asList(params);
  }

  public PDXPostProcessorDUnitTest(boolean pdxPersistent){
    this.postProcessor = PDXPostProcessor.class;
    this.pdxPersistent = pdxPersistent;
    this.jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    values = new HashMap();
  }

  @Test
  public void testRegionGet(){
    client2.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      // put in a byte value
      region.put("key2", BYTES);
    });

    client1.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      // post process for get the client domain object
      Object value = region.get("key1");
      assertTrue(value instanceof SimpleClass);

      // post process for get the raw byte value
      value = region.get("key2");
      assertTrue(Arrays.equals(BYTES, (byte[])value));
    });

    // this makes sure PostProcessor is getting called
    PDXPostProcessor pp = (PDXPostProcessor) GeodeSecurityUtil.getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

  @Test
  public void testQuery(){
    client2.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    client1.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      // post process for query
      String query = "select * from /AuthRegion";
      SelectResults result = region.query(query);

      Iterator itr = result.iterator();
      while (itr.hasNext()) {
        Object obj = itr.next();
        if(obj instanceof byte[]){
          assertTrue(Arrays.equals(BYTES, (byte[])obj));
        }
        else{
          assertTrue(obj instanceof SimpleClass);
        }
      }
    });

    // this makes sure PostProcessor is getting called
    PDXPostProcessor pp = (PDXPostProcessor) GeodeSecurityUtil.getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

  @Test
  public void testRegisterInterest(){
    client1.invoke(()->{
      ClientCache cache = new ClientCacheFactory(createClientProperties("super-user", "1234567"))
        .setPoolSubscriptionEnabled(true)
        .addPoolServer("localhost", serverPort)
        .create();

      ClientRegionFactory factory =  cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      factory.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterUpdate(EntryEvent event) {
          Object key = event.getKey();
          Object value = ((EntryEventImpl) event).getDeserializedValue();
          if(key.equals("key1")) {
            assertTrue(value instanceof SimpleClass);
          }
          else if(key.equals("key2")){
            assertTrue(Arrays.equals(BYTES, (byte[])value));
          }
        }
      });

      Region region = factory.create(REGION_NAME);
      region.put("key1", "value1");
      region.registerInterest("key1");
      region.registerInterest("key2");
    });

    client2.invoke(()->{
      ClientCache cache = createClientCache("dataUser", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    // wait for events to fire
    Awaitility.await().atMost(1, TimeUnit.SECONDS);
    PDXPostProcessor pp = (PDXPostProcessor) GeodeSecurityUtil.getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

  @Test
  public void testGfshCommand(){
    // have client2 input some domain data into the region
    client2.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      // put in a value that's a domain object
      region.put("key1", new SimpleClass(1, (byte) 1));
      // put in a byte value
      region.put("key2", BYTES);
    });

    client1.invoke(()->{
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
      if(pdxPersistent)
        assertTrue(result.getContent().toString().contains("com.gemstone.gemfire.pdx.internal.PdxInstanceImpl"));
      else
        assertTrue(result.getContent().toString().contains("SimpleClass"));

      gfsh.executeCommand("get --key=key2 --region=AuthRegion");
      result = (CommandResult)gfsh.getResult();
      assertEquals(result.getStatus(), Status.OK);
      assertTrue(result.getContent().toString().contains("byte[]"));

      gfsh.executeCommand("query --query=\"select * from /AuthRegion\"");
      result = (CommandResult)gfsh.getResult();
      System.out.println("gfsh result: " + result);
    });

    PDXPostProcessor pp = (PDXPostProcessor) GeodeSecurityUtil.getPostProcessor();
    assertEquals(pp.getCount(), 4);
  }

}
