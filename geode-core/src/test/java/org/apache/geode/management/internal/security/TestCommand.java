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
package org.apache.geode.management.internal.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.shiro.authz.Permission;

import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

public class TestCommand {

  public static ResourcePermission none = null;
  public static ResourcePermission everyOneAllowed = new ResourcePermission();
  public static ResourcePermission dataRead = new ResourcePermission(Resource.DATA, Operation.READ);
  public static ResourcePermission dataWrite =
      new ResourcePermission(Resource.DATA, Operation.WRITE);
  public static ResourcePermission dataManage =
      new ResourcePermission(Resource.DATA, Operation.MANAGE);
  public static ResourcePermission diskManage =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DISK);

  public static ResourcePermission regionARead =
      new ResourcePermission(Resource.DATA, Operation.READ, "RegionA");
  public static ResourcePermission regionAWrite =
      new ResourcePermission(Resource.DATA, Operation.WRITE, "RegionA");
  public static ResourcePermission regionAManage =
      new ResourcePermission(Resource.DATA, Operation.MANAGE, "RegionA");

  public static ResourcePermission clusterRead =
      new ResourcePermission(Resource.CLUSTER, Operation.READ);
  public static ResourcePermission clusterReadQuery =
      new ResourcePermission(Resource.CLUSTER, Operation.READ, Target.QUERY);
  public static ResourcePermission clusterWrite =
      new ResourcePermission(Resource.CLUSTER, Operation.WRITE);
  public static ResourcePermission clusterWriteDisk =
      new ResourcePermission(Resource.CLUSTER, Operation.WRITE, Target.DISK);
  public static ResourcePermission clusterManage =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE);
  public static ResourcePermission clusterManageDisk =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DISK);
  public static ResourcePermission clusterManageGateway =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.GATEWAY);
  public static ResourcePermission clusterManageJar =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.JAR);
  public static ResourcePermission clusterManageQuery =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.QUERY);

  private static List<TestCommand> testCommands = new ArrayList<>();

  static {
    init();
  }

  private final String command;
  private final ResourcePermission[] permissions;

  public TestCommand(String command, ResourcePermission... permissions) {
    this.command = command;
    this.permissions = permissions;
  }

  private static void createTestCommand(String command, ResourcePermission... permissions) {
    TestCommand instance = new TestCommand(command, permissions);
    testCommands.add(instance);
  }

  public String getCommand() {
    return this.command;
  }

  public ResourcePermission[] getPermissions() {
    return this.permissions;
  }

  public static List<TestCommand> getCommands() {
    // returns a copy of the list every time
    return testCommands.stream().collect(Collectors.toList());
  }

  public static List<TestCommand> getOnlineCommands() {
    return testCommands.stream().filter((x) -> ArrayUtils.isNotEmpty(x.getPermissions()))
        .collect(Collectors.toList());
  }

  public static List<TestCommand> getPermittedCommands(Permission permission) {
    List<TestCommand> result = new ArrayList<>();
    for (TestCommand testCommand : testCommands) {
      ResourcePermission[] cPerms = testCommand.getPermissions();
      if (cPerms == null || cPerms.length == 0) {
        // Skip offline commands.
        continue;
      }
      boolean allPermissionsAreImplied = Arrays.stream(cPerms).allMatch(permission::implies);
      if (allPermissionsAreImplied) {
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

    // CreateAlterDestroyRegionCommands
    createTestCommand("alter region --name=RegionA --eviction-max=5000", regionAManage);
    createTestCommand("create region --name=region12 --type=REPLICATE", dataManage);
    createTestCommand("create region --name=region123 --type=PARTITION_PERSISTENT", dataManage,
        clusterWriteDisk);
    // This command requires an existing persistent region named "persistentRegion"
    createTestCommand("create region --name=region1234 --template-region=/persistentRegion",
        dataManage, clusterWriteDisk);
    createTestCommand("destroy region --name=value", dataManage);

    // Data Commands
    createTestCommand("rebalance --include-region=RegionA", dataManage);
    createTestCommand("export data --region=RegionA --file=export.txt --member=exportMember",
        regionARead);
    createTestCommand("import data --region=RegionA --file=import.txt --member=importMember",
        regionAWrite);
    createTestCommand("put --key=key1 --value=value1 --region=RegionA", regionAWrite);
    createTestCommand("get --key=key1 --region=RegionA", regionARead);
    createTestCommand("remove --region=RegionA --key=key1", regionAWrite);
    createTestCommand("query --query='SELECT * FROM /RegionA'", regionARead);
    createTestCommand("locate entry --key=k1 --region=RegionA", regionARead);

    // Deploy commands
    // createTestCommand("deploy --jar=group1_functions.jar --group=Group1", dataManage); // TODO:
    // this command will fail in GfshCommandsSecurityTest at interceptor for jar file checking
    createTestCommand("undeploy --group=Group1", clusterManageJar);

    // Diskstore Commands
    createTestCommand("backup disk-store --dir=foo", dataRead, clusterWriteDisk);
    createTestCommand("list disk-stores", clusterRead);
    createTestCommand("create disk-store --name=foo --dir=bar", clusterManageDisk);
    createTestCommand("compact disk-store --name=foo", clusterManageDisk);
    createTestCommand("compact offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("upgrade offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("describe disk-store --name=foo --member=baz", clusterRead);
    createTestCommand("revoke missing-disk-store --id=foo", clusterManageDisk);
    createTestCommand("show missing-disk-stores", clusterRead);
    createTestCommand("describe offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz");
    createTestCommand("validate offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("alter disk-store --name=foo --region=xyz --disk-dirs=bar");
    createTestCommand("destroy disk-store --name=foo", clusterManageDisk);

    // DurableClientCommands
    createTestCommand("close durable-client --durable-client-id=client1", clusterManageQuery);
    createTestCommand("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1",
        clusterManageQuery);
    createTestCommand("show subscription-queue-size --durable-client-id=client1", clusterRead);
    createTestCommand("list durable-cqs --durable-client-id=client1", clusterRead);

    // ExportIMportSharedConfigurationCommands
    createTestCommand("export cluster-configuration --zip-file-name=mySharedConfig.zip",
        clusterRead);
    createTestCommand("import cluster-configuration --zip-file-name=value.zip", clusterManage);

    // FunctionCommands
    // createTestCommand("destroy function --id=InterestCalculations", dataManage);
    createTestCommand("execute function --id=InterestCalculations --groups=Group1", dataWrite);
    createTestCommand("list functions", clusterRead);

    // GfshHelpCommands
    createTestCommand("hint");
    createTestCommand("help");

    // IndexCommands
    createTestCommand("clear defined indexes", clusterManageQuery);
    createTestCommand("create defined indexes", clusterManageQuery);
    createTestCommand(
        "create index --name=myKeyIndex --expression=region1.Id --region=RegionA --type=key",
        clusterManageQuery);
    createTestCommand("define index --name=myIndex1 --expression=exp1 --region=/RegionA",
        clusterManageQuery);
    createTestCommand("destroy index --member=server2", clusterManageQuery);
    createTestCommand("destroy index --region=RegionA --member=server2", clusterManageQuery);
    createTestCommand("list indexes", clusterReadQuery);

    // LauncherLifecycleCommands
    createTestCommand("start jconsole");
    createTestCommand("start jvisualvm");
    createTestCommand("start locator --name=locator1");
    createTestCommand("start pulse");
    createTestCommand("start server --name=server1");
    createTestCommand("start vsd");
    createTestCommand("status locator");
    createTestCommand("status server");
    // createTestCommand("stop locator --name=locator1", clusterManage);
    // createTestCommand("stop server --name=server1", clusterManage);

    // MemberCommands
    createTestCommand("describe member --name=server1", clusterRead);
    createTestCommand("list members", clusterRead);

    // Misc Commands
    createTestCommand("change loglevel --loglevel=severe --members=server1", clusterWrite);
    createTestCommand("export logs --dir=data/logs", clusterRead);
    createTestCommand("export stack-traces --file=stack.txt", clusterRead);
    createTestCommand("gc", clusterManage);
    createTestCommand("netstat --member=server1", clusterRead);
    createTestCommand("show dead-locks --file=deadlocks.txt", clusterRead);
    createTestCommand("show log --member=locator1 --lines=5", clusterRead);
    createTestCommand("show metrics", clusterRead);


    // PDX Commands
    createTestCommand("configure pdx --read-serialized=true", clusterManage);
    createTestCommand(
        "pdx rename --old=org.apache --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1");

    // Queue Commands
    createTestCommand("create async-event-queue --id=myAEQ --listener=myApp.myListener",
        clusterManageJar);
    createTestCommand(
        "create async-event-queue --id=myAEQ --listener=myApp.myListener --persistent",
        clusterManageJar, clusterWriteDisk);

    createTestCommand("list async-event-queues", clusterRead);

    // RegionCommands
    createTestCommand("describe region --name=value", clusterRead);
    createTestCommand("list regions", clusterRead);

    // StatusCommands
    createTestCommand("status cluster-config-service", clusterRead);

    // Shell Commands
    createTestCommand("connect");
    createTestCommand("debug --state=on");
    createTestCommand("describe connection");
    createTestCommand("echo --string=\"Hello World!\"");
    createTestCommand("version");
    createTestCommand("sleep");
    createTestCommand("sh ls");

    // WAN Commands
    createTestCommand("create gateway-sender --id=sender1 --remote-distributed-system-id=2",
        clusterManageGateway);
    createTestCommand("start gateway-sender --id=sender1", clusterManageGateway);
    createTestCommand("pause gateway-sender --id=sender1", clusterManageGateway);
    createTestCommand("resume gateway-sender --id=sender1", clusterManageGateway);
    createTestCommand("stop gateway-sender --id=sender1", clusterManageGateway);
    createTestCommand("load-balance gateway-sender --id=sender1", clusterManageGateway);
    createTestCommand("list gateways", clusterRead);
    createTestCommand("create gateway-receiver", clusterManageGateway);
    createTestCommand("start gateway-receiver", clusterManageGateway);
    createTestCommand("stop gateway-receiver", clusterManageGateway);
    createTestCommand("status gateway-receiver", clusterRead);
    createTestCommand("status gateway-sender --id=sender1", clusterRead);

    // ShellCommand
    createTestCommand("disconnect");

    // Misc commands
    // createTestCommand("shutdown", clusterManage);
  }
}
