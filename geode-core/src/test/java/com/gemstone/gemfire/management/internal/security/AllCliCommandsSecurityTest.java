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
    commands.put("list clients", "CLIENT:LIST");
    commands.put("describe client --clientID=172.16.196.144", "CLIENT:LIST");

    // ConfigCommands
    commands.put("alter runtime", "DISTRIBUTED_SYSTEM:ALTER_RUNTIME");
    commands.put("describe config --member=Member1", "CLUSTER_CONFIGURATION:LIST");
    commands.put("export config --member=member1", "CLUSTER_CONFIGURATION:EXPORT");

    //CreateAlterDestroyRegionCommands
    commands.put("alter region --name=region1 --eviction-max=5000", "REGION:ALTER");
    commands.put("create region --name=region12", "REGION:CREATE");
    commands.put("destroy region --name=value", "REGION:DESTROY");

    //Data Commands
    commands.put("rebalance --include-region=region1", "REGION:REBALANCE");
    commands.put("export data --region=region1 --file=foo.txt --member=value", "REGION:EXPORT");
    commands.put("import data --region=region1 --file=foo.txt --member=value", "REGION:IMPORT");
    commands.put("put --key=key1 --value=value1 --region=region1", "REGION:PUT");
    commands.put("get --key=key1 --region=region1", "REGION:GET");
    commands.put("remove --region=region1", "REGION:DELETE");
    commands.put("query --query='SELECT * FROM /region1'", "QUERY:EXECUTE");

    // Deploy commands
    commands.put("deploy --jar=group1_functions.jar --group=Group1", "FUNCTION:DEPLOY");
    commands.put("list deployed", "FUNCTION:LIST");
    commands.put("undeploy --group=Group1", "FUNCTION:UNDEPLOY");

    // Diskstore Commands
    commands.put("backup disk-store --dir=foo", "DISKSTORE:MANAGE");
    commands.put("list disk-stores", "DISKSTORE:LIST");
    commands.put("create disk-store --name=foo --dir=bar", "DISKSTORE:MANAGE");
    commands.put("compact disk-store --name=foo", "DISKSTORE:MANAGE");
    commands.put("compact offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:MANAGE");
    commands.put("upgrade offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:MANAGE");
    commands.put("describe disk-store --name=foo --member=baz", "DISKSTORE:LIST");
    commands.put("revoke missing-disk-store --id=foo", "DISKSTORE:MANAGE");
    commands.put("show missing-disk-stores", "DISKSTORE:MANAGE");
    commands.put("describe offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:LIST");
    commands.put("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz", "DISKSTORE:MANAGE");
    commands.put("validate offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:MANAGE");
    commands.put("alter disk-store --name=foo --region=xyz --disk-dirs=bar", "DISKSTORE:MANAGE");
    commands.put("destroy disk-store --name=foo", "DISKSTORE:MANAGE");

    // DurableClientCommands
    commands.put("close durable-client --durable-client-id=client1", "CONTINUOUS_QUERY:STOP");
    commands.put("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1", "CONTINUOUS_QUERY:STOP");
    commands.put("show subscription-queue-size --durable-client-id=client1", "CONTINUOUS_QUERY:LIST");
    commands.put("list durable-cqs --durable-client-id=client1", "CONTINUOUS_QUERY:LIST");

    //ExportIMportSharedConfigurationCommands
    commands.put("export cluster-configuration --zip-file-name=mySharedConfig.zip", "CLUSTER_CONFIGURATION:EXPORT");
    commands.put("import cluster-configuration --zip-file-name=value", "CLUSTER_CONFIGURATION:IMPORT");

    //FunctionCommands
    commands.put("destroy function --id=InterestCalculations", "FUNCTION:DESTROY");
    commands.put("execute function --id=InterestCalculations --group=Group1", "FUNCTION:EXECUTE");
    commands.put("list functions", "FUNCTION:LIST");

    //GfshHelpCommands
    commands.put("hint", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("help", "DISTRIBUTED_SYSTEM:ALL");

    //IndexCommands
    commands.put("clear defined indexes", "INDEX:FLUSH");
    commands.put("create defined indexes", "INDEX:CREATE");
    commands.put("create index --name=myKeyIndex --expression=region1.Id --region=region1 --type=key", "INDEX:CREATE");
    commands.put("define index --name=myIndex1 --expression=exp1 --region=/exampleRegion", "INDEX:CREATE");
    commands.put("destroy index --member=server2", "INDEX:DESTROY");
    commands.put("list indexes", "INDEX:LIST");

    //LauncherLifecycleCommands
    commands.put("start jconsole", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("start jvisualvm", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("start locator --name=locator1", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("start pulse", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("start server --name=server1", "MEMBER:START");
    commands.put("start vsd", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("status locator", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("status server", "MEMBER:STATUS");
    commands.put("stop locator --name=locator1", "LOCATOR:STOP");
    commands.put("stop server --name=server1", "MEMBER:STOP");

    //MemberCommands
    commands.put("describe member --name=server1", "MEMBER:LIST");
    commands.put("list members", "MEMBER:LIST");

    // Misc Commands
    commands.put("change loglevel --loglevel=severe --member=server1", "DISTRIBUTED_SYSTEM:MANAGE");
    commands.put("export logs --dir=data/logs", "DISTRIBUTED_SYSTEM:LIST");
    commands.put("export stack-traces --file=stack.txt", "DISTRIBUTED_SYSTEM:LIST");
    commands.put("gc", "DISTRIBUTED_SYSTEM:MANAGE");
    commands.put("netstat --member=server1", "DISTRIBUTED_SYSTEM:MANAGE");
    commands.put("show dead-locks --file=deadlocks.txt", "DISTRIBUTED_SYSTEM:LIST");
    commands.put("show log --member=locator1 --lines=5", "DISTRIBUTED_SYSTEM:LIST");
    commands.put("show metrics", "DISTRIBUTED_SYSTEM:LIST");


    // PDX Commands
    commands.put("configure pdx --read-serialized=true", "PDX:MANAGE");
    commands.put("pdx rename --old=com.gemstone --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1", "PDX:MANAGE");

    // Queue Commands
    commands.put("create async-event-queue --id=myAEQ --listener=myApp.myListener", "ASYNC_EVENT_QUEUE:MANAGE");
    commands.put("list async-event-queues", "ASYNC_EVENT_QUEUE:LIST");

    //RegionCommands
    commands.put("describe region --name=value", "REGION:LIST");
    commands.put("list regions", "REGION:LIST");

    // StatusCommands
    commands.put("status cluster-config-service", "CLUSTER_CONFIGURATION:STATUS");

    // Shell Commands
    commands.put("connect", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("debug --state=on", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("describe connection", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("echo --string=\"Hello World!\"", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("encrypt password --password=value", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("version", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("sleep", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("sh ls", "DISTRIBUTED_SYSTEM:ALL");


    // WAN Commands
    commands.put("create gateway-sender --id=sender1 --remote-distributed-system-id=2", "GATEWAY:MANAGE");
    commands.put("start gateway-sender --id=sender1", "GATEWAY:MANAGE");
    commands.put("pause gateway-sender --id=sender1", "GATEWAY:MANAGE");
    commands.put("resume gateway-sender --id=sender1", "GATEWAY:MANAGE");
    commands.put("stop gateway-sender --id=sender1", "GATEWAY:MANAGE");
    commands.put("load-balance gateway-sender --id=sender1", "GATEWAY:MANAGE");
    commands.put("list gateways", "GATEWAY:LIST");
    commands.put("create gateway-receiver", "GATEWAY:MANAGE");
    commands.put("start gateway-receiver", "GATEWAY:MANAGE");
    commands.put("stop gateway-receiver", "GATEWAY:MANAGE");
    commands.put("status gateway-receiver", "GATEWAY:LIST");

    commands.put("disconnect", "DISTRIBUTED_SYSTEM:ALL");
    commands.put("shutdown", "DISTRIBUTED_SYSTEM:MANAGE");
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
  // run this test first
  public void a_testNoAccess(){
    for (Map.Entry<String, String> perm : commands.entrySet()) {
      LogService.getLogger().info("processing: "+perm.getKey());
      assertThatThrownBy(() -> bean.processCommand(perm.getKey()))
          .hasMessageStartingWith("Access Denied: Not authorized for " + perm.getValue())
          .isInstanceOf(SecurityException.class);
    }
  }

  @Test
  @JMXConnectionConfiguration(user = "adminUser", password = "1234567")
  public void b_testBAdminUser() throws Exception {
    for (String cmd : commands.keySet()) {
      LogService.getLogger().info("processing: "+cmd);
      bean.processCommand(cmd);
    }
  }


}
