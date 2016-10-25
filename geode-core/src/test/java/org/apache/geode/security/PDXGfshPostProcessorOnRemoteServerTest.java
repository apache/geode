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

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class PDXGfshPostProcessorOnRemoteServerTest extends JUnit4DistributedTestCase {
  protected static final String REGION_NAME = "AuthRegion";
  protected VM locator = null;
  protected VM server = null;

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    this.locator = host.getVM(0);
    this.server = host.getVM(1);
  }

  @Test
  public void testGfshCommand() throws Exception{
    // set up locator with security
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int jmxPort = ports[1];
    locator.invoke(()->{
      Properties props = new Properties();
      props.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/clientServer.json");
      props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
      props.setProperty(MCAST_PORT, "0");
      props.put(JMX_MANAGER, "true");
      props.put(JMX_MANAGER_START, "true");
      props.put(JMX_MANAGER_PORT, jmxPort+"");
      props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
      Locator.startLocatorAndDS(locatorPort, new File("locator.log"), props);
    });

    // set up server with security
    String locators = "localhost[" + locatorPort + "]";
    server.invoke(()->{
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/clientServer.json");
      props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
      props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
      props.setProperty(USE_CLUSTER_CONFIGURATION, "true");

      // the following are needed for peer-to-peer authentication
      props.setProperty("security-username", "super-user");
      props.setProperty("security-password", "1234567");
      InternalDistributedSystem ds = getSystem(props);

      Cache cache = CacheFactory.create(ds);
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);

      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.start();

      for(int i=0; i<5; i++){
        SimpleClass obj = new SimpleClass(i, (byte)i);
        region.put("key"+i, obj);
      }
    });

    // wait until the region bean is visible
    locator.invoke(()->{
      Awaitility.await().pollInterval(500, TimeUnit.MICROSECONDS).atMost(5, TimeUnit.SECONDS).until(()->{
        Cache cache = CacheFactory.getAnyInstance();
        Object bean = ManagementService.getManagementService(cache).getDistributedRegionMXBean("/"+REGION_NAME);
        return bean != null;
      });
    });

    // run gfsh command in this vm
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
    CommandResult result = (CommandResult)gfsh.getResult();

    // get command
    gfsh.executeCommand("get --key=key1 --region=AuthRegion");
    result = (CommandResult) gfsh.getResult();
    assertEquals(result.getStatus(), Status.OK);
    assertTrue(result.getContent().toString().contains(SimpleClass.class.getName()));

    gfsh.executeCommand("query --query=\"select * from /AuthRegion\"");
    result = (CommandResult)gfsh.getResult();

    CliUtil.isGfshVM = false;
    server.invoke(()-> {
      PDXPostProcessor pp = (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
      // verify that the post processor is called 6 times. (5 for the query, 1 for the get)
      assertEquals(pp.getCount(), 6);
    });
  }

}
