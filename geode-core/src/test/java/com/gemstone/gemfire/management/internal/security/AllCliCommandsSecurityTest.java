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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category(IntegrationTest.class)
public class AllCliCommandsSecurityTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;

  private Map<String, String> perms = new HashMap<>();

  public AllCliCommandsSecurityTest() {
    // ClientCommands
    perms.put("list clients", "CLIENT:LIST");
    perms.put("describe client --clientID=172.16.196.144", "CLIENT:LIST");

    // ConfigCommands
    perms.put("alter runtime", "DISTRIBUTED_SYSTEM:ALTER_RUNTIME");
    perms.put("describe config --member=Member1", "CLUSTER_CONFIGURATION:LIST");
    perms.put("export config --member=member1", "CLUSTER_CONFIGURATION:EXPORT");

    //CreateAlterDestroyRegionCommands
    perms.put("alter region --name=region1 --eviction-max=5000", "REGION:ALTER");
    perms.put("create region --name=region12", "REGION:CREATE");
    perms.put("destroy region --name=value", "REGION:DESTROY");

    //Data Commands
    perms.put("rebalance --include-region=region1", "REGION:REBALANCE");
    perms.put("export data --region=region1 --file=foo.txt --member=value", "REGION:EXPORT");
    perms.put("import data --region=region1 --file=foo.txt --member=value", "REGION:IMPORT");
    perms.put("put --key=key1 --value=value1 --region=region1", "REGION:PUT");
    perms.put("get --key=key1 --region=region1", "REGION:GET");
    perms.put("remove --region=region1", "REGION:DELETE");
    perms.put("query --query='SELECT * FROM /region1'", "QUERY:EXECUTE");

    // Deploy commands
    perms.put("deploy --jar=group1_functions.jar --group=Group1", "FUNCTION:DEPLOY");
    perms.put("list deployed", "FUNCTION:LIST");
    perms.put("undeploy --group=Group1", "FUNCTION:UNDEPLOY");

    // Diskstore Commands
    perms.put("backup disk-store --dir=foo", "DISKSTORE:MANAGE");
    perms.put("list disk-stores", "DISKSTORE:LIST");
    perms.put("create disk-store --name=foo --dir=bar", "DISKSTORE:MANAGE");
    perms.put("compact disk-store --name=foo", "DISKSTORE:MANAGE");
    perms.put("compact offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:MANAGE");
    perms.put("upgrade offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:MANAGE");
    perms.put("describe disk-store --name=foo --member=baz", "DISKSTORE:LIST");
    perms.put("revoke missing-disk-store --id=foo", "DISKSTORE:MANAGE");
    perms.put("show missing-disk-stores", "DISKSTORE:MANAGE");
    perms.put("describe offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:LIST");
    perms.put("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz", "DISKSTORE:MANAGE");
    perms.put("validate offline-disk-store --name=foo --disk-dirs=bar", "DISKSTORE:MANAGE");
    //    perms.put("alter offline-disk-store --name=foo --region=xyz --disk-dirs=bar", DISKSTORE_MANAGE);
    perms.put("destroy disk-store --name=foo", "DISKSTORE:MANAGE");

    // DurableClientCommands
    perms.put("close durable-client --durable-client-id=client1", "CONTINUOUS_QUERY:STOP");
    perms.put("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1", "CONTINUOUS_QUERY:STOP");
    perms.put("show subscription-queue-size --durable-client-id=client1", "CONTINUOUS_QUERY:LIST");
    perms.put("list durable-cqs --durable-client-id=client1", "CONTINUOUS_QUERY:LIST");

    //ExportIMportSharedConfigurationCommands
    perms.put("export cluster-configuration --zip-file-name=mySharedConfig.zip", "CLUSTER_CONFIGURATION:EXPORT");
    perms.put("import cluster-configuration --zip-file-name=value", "CLUSTER_CONFIGURATION:IMPORT");

    //FunctionCommands
    perms.put("destroy function --id=InterestCalculations", "FUNCTION:DESTROY");
    perms.put("execute function --id=InterestCalculations --group=Group1", "FUNCTION:EXECUTE");
    perms.put("list functions", "FUNCTION:LIST");

    //GfshHelpCommands
    perms.put("hint", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("help", "DISTRIBUTED_SYSTEM:ALL");

    //IndexCommands
    perms.put("clear defined indexes", "INDEX:FLUSH");
    perms.put("create defined indexes", "INDEX:CREATE");
    perms.put("create index --name=myKeyIndex --expression=region1.Id --region=region1 --type=key", "INDEX:CREATE");
    perms.put("define index --name=myIndex1 --expression=exp1 --region=/exampleRegion", "INDEX:CREATE");
    perms.put("destroy index --member=server2", "INDEX:DESTROY");
    perms.put("list indexes", "INDEX:LIST");

    //LauncherLifecycleCommands
    perms.put("start jconsole", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("start jvisualvm", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("start locator --name=locator1", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("start pulse", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("start server --name=server1", "MEMBER:START");
    perms.put("start vsd", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("status locator", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("status server", "MEMBER:STATUS");
    perms.put("stop locator --name=locator1", "LOCATOR:STOP");
    perms.put("stop server --name=server1", "MEMBER:STOP");

    //MemberCommands
    perms.put("describe member --name=server1", "MEMBER:LIST");
    perms.put("list members", "MEMBER:LIST");

    // Misc Commands
    perms.put("change loglevel --loglevel=severe --member=server1", "DISTRIBUTED_SYSTEM:MANAGE");
    perms.put("export logs --dir=data/logs", "DISTRIBUTED_SYSTEM:LIST");
    perms.put("export stack-traces --file=stack.txt", "DISTRIBUTED_SYSTEM:LIST");
    perms.put("gc", "DISTRIBUTED_SYSTEM:MANAGE");
    perms.put("netstat --member=server1", "DISTRIBUTED_SYSTEM:MANAGE");
    perms.put("show dead-locks --file=deadlocks.txt", "DISTRIBUTED_SYSTEM:LIST");
    perms.put("show log --member=locator1 --lines=5", "DISTRIBUTED_SYSTEM:LIST");
    perms.put("show metrics", "DISTRIBUTED_SYSTEM:LIST");
    //    perms.put("shutdown", DISTRIBUTED_SYSTEM_MANAGE);

    // PDX Commands
    perms.put("configure pdx --read-serialized=true", "PDX:MANAGE");
    perms.put("pdx rename --old=com.gemstone --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1", "PDX:MANAGE");

    // Queue Commands
    perms.put("create async-event-queue --id=myAEQ --listener=myApp.myListener", "ASYNC_EVENT_QUEUE:MANAGE");
    perms.put("list async-event-queues", "ASYNC_EVENT_QUEUE:LIST");

    //RegionCommands
    perms.put("describe region --name=value", "REGION:LIST");
    perms.put("list regions", "REGION:LIST");

    // StatusCommands
    perms.put("status cluster-config-service", "CLUSTER_CONFIGURATION:STATUS");

    // Shell Commands
    perms.put("connect", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("debug --state=on", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("describe connection", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("echo --string=\"Hello World!\"", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("encrypt password --password=value", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("version", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("sleep", "DISTRIBUTED_SYSTEM:ALL");
    perms.put("sh ls", "DISTRIBUTED_SYSTEM:ALL");
    // perms.put("disconnect", "DISTRIBUTED_SYSTEM:ALL");

    // WAN Commands
    perms.put("create gateway-sender --id=sender1 --remote-distributed-system-id=2", "GATEWAY:MANAGE");
    perms.put("start gateway-sender --id=sender1", "GATEWAY:MANAGE");
    perms.put("pause gateway-sender --id=sender1", "GATEWAY:MANAGE");
    perms.put("resume gateway-sender --id=sender1", "GATEWAY:MANAGE");
    perms.put("stop gateway-sender --id=sender1", "GATEWAY:MANAGE");
    perms.put("load-balance gateway-sender --id=sender1", "GATEWAY:MANAGE");
    perms.put("list gateways", "GATEWAY:LIST");
    perms.put("create gateway-receiver", "GATEWAY:MANAGE");
    perms.put("start gateway-receiver", "GATEWAY:MANAGE");
    perms.put("stop gateway-receiver", "GATEWAY:MANAGE");
    perms.put("status gateway-receiver", "GATEWAY:LIST");
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
  @JMXConnectionConfiguration(user = "adminUser", password = "1234567")
  public void testAdminUser() throws Exception {
    for (String cmd : perms.keySet()) {
      LogService.getLogger().info("processing: "+cmd);
      bean.processCommand(cmd);
    }
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess(){
    for (Map.Entry<String, String> perm : perms.entrySet()) {
      LogService.getLogger().info("processing: "+perm.getKey());
      assertThatThrownBy(() -> bean.processCommand(perm.getKey()))
            .hasMessageStartingWith("Access Denied: Not authorized for " + perm.getValue())
            .isInstanceOf(SecurityException.class);
    }
  }

}
