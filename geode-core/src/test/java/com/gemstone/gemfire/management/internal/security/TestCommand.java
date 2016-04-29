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

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.operations.OperationContext;

import org.apache.shiro.authz.Permission;

public class TestCommand {
  public static OperationContext none = null;
  public static OperationContext everyOneAllowed = new ResourceOperationContext();
  public static OperationContext dataRead = new ResourceOperationContext("DATA", "READ");
  public static OperationContext dataWrite = new ResourceOperationContext("DATA", "WRITE");
  public static OperationContext dataManage = new ResourceOperationContext("DATA", "MANAGE");

  public static OperationContext regionARead = new ResourceOperationContext("DATA", "READ", "RegionA");
  public static OperationContext regionAWrite = new ResourceOperationContext("DATA", "WRITE", "RegionA");

  public static OperationContext clusterRead = new ResourceOperationContext("CLUSTER", "READ");
  public static OperationContext clusterWrite = new ResourceOperationContext("CLUSTER", "WRITE");
  public static OperationContext clusterManage = new ResourceOperationContext("CLUSTER", "MANAGE");

  private static List<TestCommand> testCommands = new ArrayList<>();

  static{
    init();
  }
  
  private final String command;
  private final OperationContext permission;
  
  public TestCommand(String command, OperationContext permission) {
    this.command = command;
    this.permission = permission;
  }
  
  private static void createTestCommand(String command, OperationContext permission) {
    TestCommand instance = new TestCommand(command, permission);
    testCommands.add(instance);
  }
  
  public String getCommand() {
    return this.command;
  }

  public OperationContext getPermission() {
    return this.permission;
  }

  public static List<TestCommand> getCommands(){
    return testCommands;
  }

  public static List<TestCommand> getPermittedCommands(Permission permission){
    List<TestCommand> result = new ArrayList<>();
    for(TestCommand testCommand:testCommands){
      OperationContext cPerm = testCommand.getPermission();
      if(cPerm!=null && permission.implies(cPerm)){
        result.add(testCommand);
      }
    }
    return result;
  }

  private static void init() {
    // ClientCommands
    createTestCommand("list clients", clusterRead);
    createTestCommand("describe client --clientID=172.16.196.144", clusterRead);

    // ConfigCommands
    createTestCommand("alter runtime", clusterManage);
    createTestCommand("describe config --member=Member1", clusterRead);
    createTestCommand("export config --member=member1", clusterRead);

    //CreateAlterDestroyRegionCommands
    createTestCommand("alter region --name=region1 --eviction-max=5000", dataManage);
    createTestCommand("create region --name=region12 --type=REPLICATE", dataManage);
    createTestCommand("destroy region --name=value", dataManage);

    //Data Commands
    createTestCommand("rebalance --include-region=regionA", dataManage);
    createTestCommand("export data --region=regionA --file=export.txt --member=exportMember", regionARead);
    createTestCommand("import data --region=regionA --file=import.txt --member=importMember", regionAWrite);
    createTestCommand("put --key=key1 --value=value1 --region=regionA", regionAWrite);
    createTestCommand("get --key=key1 --region=regionA", regionARead);
    createTestCommand("remove --region=regionA", dataManage);
    createTestCommand("query --query='SELECT * FROM /region1'", dataRead);
    createTestCommand("locate entry --key=k1 --region=regionA", regionARead);

    // Deploy commands
    //createTestCommand("deploy --jar=group1_functions.jar --group=Group1", dataManage); // TODO: this command will fail in GfshCommandsSecurityTest at interceptor for jar file checking
    createTestCommand("undeploy --group=Group1", dataManage);

    // Diskstore Commands
    createTestCommand("backup disk-store --dir=foo", dataRead);
    createTestCommand("list disk-stores", clusterRead);
    createTestCommand("create disk-store --name=foo --dir=bar", dataManage);
    createTestCommand("compact disk-store --name=foo", dataManage);
    createTestCommand("compact offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("upgrade offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("describe disk-store --name=foo --member=baz", clusterRead);
    createTestCommand("revoke missing-disk-store --id=foo", dataManage);
    createTestCommand("show missing-disk-stores", clusterRead);
    createTestCommand("describe offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz", null);
    createTestCommand("validate offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("alter disk-store --name=foo --region=xyz --disk-dirs=bar", null);
    createTestCommand("destroy disk-store --name=foo", dataManage);

    // DurableClientCommands
    createTestCommand("close durable-client --durable-client-id=client1", dataManage);
    createTestCommand("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1", dataManage);
    createTestCommand("show subscription-queue-size --durable-client-id=client1", clusterRead);
    createTestCommand("list durable-cqs --durable-client-id=client1", clusterRead);

    //ExportIMportSharedConfigurationCommands
    createTestCommand("export cluster-configuration --zip-file-name=mySharedConfig.zip", clusterRead);
    createTestCommand("import cluster-configuration --zip-file-name=value.zip", clusterManage);

    //FunctionCommands
    //createTestCommand("destroy function --id=InterestCalculations", dataManage);
    createTestCommand("execute function --id=InterestCalculations --group=Group1", dataWrite);
    createTestCommand("list functions", clusterRead);

    //GfshHelpCommands
    createTestCommand("hint", null);
    createTestCommand("help", null);

    //IndexCommands
    createTestCommand("clear defined indexes", dataManage);
    createTestCommand("create defined indexes", dataManage);
    createTestCommand("create index --name=myKeyIndex --expression=region1.Id --region=region1 --type=key", dataManage);
    createTestCommand("define index --name=myIndex1 --expression=exp1 --region=/exampleRegion", dataManage);
    createTestCommand("destroy index --member=server2", dataManage);
    createTestCommand("list indexes", clusterRead);

    //LauncherLifecycleCommands
    createTestCommand("start jconsole", null);
    createTestCommand("start jvisualvm", null);
    createTestCommand("start locator --name=locator1", null);
    createTestCommand("start pulse", null);
    createTestCommand("start server --name=server1", null);
    createTestCommand("start vsd", null);
    createTestCommand("status locator", null);
    createTestCommand("status server", null);
    //createTestCommand("stop locator --name=locator1", clusterManage);
    //createTestCommand("stop server --name=server1", clusterManage);

    //MemberCommands
    createTestCommand("describe member --name=server1", clusterRead);
    createTestCommand("list members", clusterRead);

    // Misc Commands
    createTestCommand("change loglevel --loglevel=severe --member=server1", clusterWrite);
    createTestCommand("export logs --dir=data/logs", clusterRead);
    createTestCommand("export stack-traces --file=stack.txt", clusterRead);
    createTestCommand("gc", clusterManage);
    createTestCommand("netstat --member=server1", clusterRead);
    createTestCommand("show dead-locks --file=deadlocks.txt", clusterRead);
    createTestCommand("show log --member=locator1 --lines=5", clusterRead);
    createTestCommand("show metrics", clusterRead);


    // PDX Commands
    createTestCommand("configure pdx --read-serialized=true", dataManage);
    //createTestCommand("pdx rename --old=com.gemstone --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1", dataManage);

    // Queue Commands
    createTestCommand("create async-event-queue --id=myAEQ --listener=myApp.myListener", dataManage);
    createTestCommand("list async-event-queues", clusterRead);

    //RegionCommands
    createTestCommand("describe region --name=value", clusterRead);
    createTestCommand("list regions", clusterRead);

    // StatusCommands
    createTestCommand("status cluster-config-service", clusterRead);

    // Shell Commands
    createTestCommand("connect", null);
    createTestCommand("debug --state=on", null);
    createTestCommand("describe connection", null);
    createTestCommand("echo --string=\"Hello World!\"", null);
    createTestCommand("encrypt password --password=value", null);
    createTestCommand("version", null);
    createTestCommand("sleep", null);
    createTestCommand("sh ls", null);


    // WAN Commands
    createTestCommand("create gateway-sender --id=sender1 --remote-distributed-system-id=2", dataManage);
    createTestCommand("start gateway-sender --id=sender1", dataManage);
    createTestCommand("pause gateway-sender --id=sender1", dataManage);
    createTestCommand("resume gateway-sender --id=sender1", dataManage);
    createTestCommand("stop gateway-sender --id=sender1", dataManage);
    createTestCommand("load-balance gateway-sender --id=sender1", dataManage);
    createTestCommand("list gateways", clusterRead);
    createTestCommand("create gateway-receiver", dataManage);
    createTestCommand("start gateway-receiver", dataManage);
    createTestCommand("stop gateway-receiver", dataManage);
    createTestCommand("status gateway-receiver", clusterRead);
    createTestCommand("status gateway-sender --id=sender1", clusterRead);

    //ShellCommand
    createTestCommand("disconnect", null);
    //Misc commands
    //createTestCommand("shutdown", clusterManage);
  };
}
