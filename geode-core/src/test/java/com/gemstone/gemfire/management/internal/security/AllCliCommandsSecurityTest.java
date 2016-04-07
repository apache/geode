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
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * tests will be run alphabetically, in this test class, we run non-admin test first,
 * since we don't want to have the server stopped for the rest of the tests.
 */

@Category(IntegrationTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AllCliCommandsSecurityTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;

  // use LinkedHashMap to preserve order. We need to execute shutdown command last
  private Map<String, String> commands = new LinkedHashMap<>();

  public AllCliCommandsSecurityTest() {
    // ClientCommands
    commands.put("list clients", "CLUSTER:READ");
    commands.put("describe client --clientID=172.16.196.144", "CLUSTER:READ");

    // ConfigCommands
    commands.put("alter runtime", "CLUSTER:MANAGE");
    commands.put("describe config --member=Member1", "CLUSTER:READ");
    commands.put("export config --member=member1", "CLUSTER:READ");

    //CreateAlterDestroyRegionCommands
    commands.put("alter region --name=region1 --eviction-max=5000", "DATA:MANAGE");
    commands.put("create region --name=region12", "DATA:MANAGE");
    commands.put("destroy region --name=value", "DATA:MANAGE");

    //Data Commands
    commands.put("rebalance --include-region=region1", "DATA:MANAGE");
    commands.put("export data --region=region1 --file=foo.txt --member=value", "DATA:READ");
    commands.put("import data --region=region1 --file=foo.txt --member=value", "DATA:WRITE");
    commands.put("put --key=key1 --value=value1 --region=region1", "DATA:WRITE");
    commands.put("get --key=key1 --region=region1", "DATA:READ");
    commands.put("remove --region=region1", "DATA:MANAGE");
    commands.put("query --query='SELECT * FROM /region1'", "DATA:READ");

    // Deploy commands
    commands.put("deploy --jar=group1_functions.jar --group=Group1", "DATA:MANAGE");
    commands.put("list deployed", "CLUSTER:READ");
    commands.put("undeploy --group=Group1", "DATA:MANAGE");

    // Diskstore Commands
    commands.put("backup disk-store --dir=foo", "DATA:READ");
    commands.put("list disk-stores", "CLUSTER:READ");
    commands.put("create disk-store --name=foo --dir=bar", "DATA:MANAGE");
    commands.put("compact disk-store --name=foo", "DATA:MANAGE");
    commands.put("compact offline-disk-store --name=foo --disk-dirs=bar", null);
    commands.put("upgrade offline-disk-store --name=foo --disk-dirs=bar", null);
    commands.put("describe disk-store --name=foo --member=baz", "CLUSTER:READ");
    commands.put("revoke missing-disk-store --id=foo", "DATA:MANAGE");
    commands.put("show missing-disk-stores", "CLUSTER:READ");
    commands.put("describe offline-disk-store --name=foo --disk-dirs=bar", null);
    commands.put("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz", null);
    commands.put("validate offline-disk-store --name=foo --disk-dirs=bar", null);
    commands.put("alter disk-store --name=foo --region=xyz --disk-dirs=bar", null); // alteroffline
    commands.put("destroy disk-store --name=foo", "DATA:MANAGE");

    // DurableClientCommands
    commands.put("close durable-client --durable-client-id=client1", "DATA:MANAGE");
    commands.put("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1", "DATA:MANAGE");
    commands.put("show subscription-queue-size --durable-client-id=client1", "CLUSTER:READ");
    commands.put("list durable-cqs --durable-client-id=client1", "CLUSTER:READ");

    //ExportIMportSharedConfigurationCommands
    commands.put("export cluster-configuration --zip-file-name=mySharedConfig.zip", "CLUSTER:READ");
    commands.put("import cluster-configuration --zip-file-name=value", "CLUSTER:MANAGE");

    //FunctionCommands
    commands.put("destroy function --id=InterestCalculations", "DATA:MANAGE");
    commands.put("execute function --id=InterestCalculations --group=Group1", "DATA:WRITE");
    commands.put("list functions", "CLUSTER:READ");

    //GfshHelpCommands
    commands.put("hint", null);
    commands.put("help", null);

    //IndexCommands
    commands.put("clear defined indexes", "DATA:MANAGE");
    commands.put("create defined indexes", "DATA:MANAGE");
    commands.put("create index --name=myKeyIndex --expression=region1.Id --region=region1 --type=key", "DATA:MANAGE");
    commands.put("define index --name=myIndex1 --expression=exp1 --region=/exampleRegion", "DATA:MANAGE");
    commands.put("destroy index --member=server2", "DATA:MANAGE");
    commands.put("list indexes", "CLUSTER:READ");

    //LauncherLifecycleCommands
    commands.put("start jconsole", null);
    commands.put("start jvisualvm", null);
    commands.put("start locator --name=locator1", null);
    commands.put("start pulse", null);
    commands.put("start server --name=server1", null);
    commands.put("start vsd", null);
    commands.put("status locator", null);
    commands.put("status server", null);
    commands.put("stop locator --name=locator1", "CLUSTER:MANAGE");
    commands.put("stop server --name=server1", "CLUSTER:MANAGE");

    //MemberCommands
    commands.put("describe member --name=server1", "CLUSTER:READ");
    commands.put("list members", "CLUSTER:READ");

    // Misc Commands
    commands.put("change loglevel --loglevel=severe --member=server1", "CLUSTER:WRITE");
    commands.put("export logs --dir=data/logs", "CLUSTER:READ");
    commands.put("export stack-traces --file=stack.txt", "CLUSTER:READ");
    commands.put("gc", "CLUSTER:MANAGE");
    commands.put("netstat --member=server1", "CLUSTER:READ");
    commands.put("show dead-locks --file=deadlocks.txt", "CLUSTER:READ");
    commands.put("show log --member=locator1 --lines=5", "CLUSTER:READ");
    commands.put("show metrics", "CLUSTER:READ");


    // PDX Commands
    commands.put("configure pdx --read-serialized=true", "DATA:MANAGE");
    commands.put("pdx rename --old=com.gemstone --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1", "DATA:MANAGE");

    // Queue Commands
    commands.put("create async-event-queue --id=myAEQ --listener=myApp.myListener", "DATA:MANAGE");
    commands.put("list async-event-queues", "CLUSTER:READ");

    //RegionCommands
    commands.put("describe region --name=value", "CLUSTER:READ");
    commands.put("list regions", "CLUSTER:READ");

    // StatusCommands
    commands.put("status cluster-config-service", "CLUSTER:READ");

    // Shell Commands
    commands.put("connect", null);
    commands.put("debug --state=on", null);
    commands.put("describe connection", null);
    commands.put("echo --string=\"Hello World!\"", null);
    commands.put("encrypt password --password=value", null);
    commands.put("version", null);
    commands.put("sleep", null);
    commands.put("sh ls", null);


    // WAN Commands
    commands.put("create gateway-sender --id=sender1 --remote-distributed-system-id=2", "DATA:MANAGE");
    commands.put("start gateway-sender --id=sender1", "DATA:MANAGE");
    commands.put("pause gateway-sender --id=sender1", "DATA:MANAGE");
    commands.put("resume gateway-sender --id=sender1", "DATA:MANAGE");
    commands.put("stop gateway-sender --id=sender1", "DATA:MANAGE");
    commands.put("load-balance gateway-sender --id=sender1", "DATA:MANAGE");
    commands.put("list gateways", "CLUSTER:READ");
    commands.put("create gateway-receiver", "DATA:MANAGE");
    commands.put("start gateway-receiver", "DATA:MANAGE");
    commands.put("stop gateway-receiver", "DATA:MANAGE");
    commands.put("status gateway-receiver", "CLUSTER:READ");
    commands.put("status gateway-sender --id=sender1", "CLUSTER:READ");

    //ShellCommand
    commands.put("disconnect", null);
    //Misc commands
    commands.put("shutdown", "CLUSTER:MANAGE");
  }

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  // the tests are run in alphabetical order, so the naming of the tests do matter
  public void a_testNoAccess(){
    for (Map.Entry<String, String> perm : commands.entrySet()) {
      LogService.getLogger().info("processing: "+perm.getKey());
      // for those commands that don't require any permission, any user can execute them
      if(perm.getValue()==null){
        bean.processCommand(perm.getKey());
      }
      else {
        assertThatThrownBy(() -> bean.processCommand(perm.getKey()))
            .hasMessageContaining(perm.getValue())
            .isInstanceOf(SecurityException.class);
      }
    }
  }

  @Test
  @JMXConnectionConfiguration(user = "super-user", password = "1234567")
  public void b_testAdminUser() throws Exception {
    for (String cmd : commands.keySet()) {
      LogService.getLogger().info("processing: "+cmd);
      bean.processCommand(cmd);
    }
  }


}
