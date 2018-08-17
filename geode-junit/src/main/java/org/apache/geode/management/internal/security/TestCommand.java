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
  public static ResourcePermission diskManage =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DISK);

  public static ResourcePermission regionARead =
      new ResourcePermission(Resource.DATA, Operation.READ, "RegionA");
  public static ResourcePermission regionAWrite =
      new ResourcePermission(Resource.DATA, Operation.WRITE, "RegionA");
  public static ResourcePermission regionAManage =
      new ResourcePermission(Resource.DATA, Operation.MANAGE, "RegionA");

  public static ResourcePermission clusterReadQuery =
      new ResourcePermission(Resource.CLUSTER, Operation.READ, Target.QUERY);
  public static ResourcePermission clusterWriteDisk =
      new ResourcePermission(Resource.CLUSTER, Operation.WRITE, Target.DISK);
  public static ResourcePermission clusterManageDisk =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DISK);
  public static ResourcePermission clusterManageGateway =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.GATEWAY);
  public static ResourcePermission clusterManageDeploy =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DEPLOY);
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
    // ListClientCommand, DescribeClientCommand
    createTestCommand("list clients", ResourcePermissions.CLUSTER_READ);
    createTestCommand("describe client --clientID=172.16.196.144",
        ResourcePermissions.CLUSTER_READ);

    // AlterRuntimeConfigCommand, DescribeConfigCommand, ExportConfigCommand (config commands)
    createTestCommand("alter runtime", ResourcePermissions.CLUSTER_MANAGE);
    createTestCommand("describe config --member=Member1", ResourcePermissions.CLUSTER_READ);
    createTestCommand("export config --member=member1", ResourcePermissions.CLUSTER_READ);

    // CreateRegionCommand, AlterRegionCommand, DestroyRegionCommand
    createTestCommand("alter region --name=RegionA --eviction-max=5000", regionAManage);
    createTestCommand("create region --name=region12 --type=REPLICATE",
        ResourcePermissions.DATA_MANAGE);
    createTestCommand("create region --name=region123 --type=PARTITION_PERSISTENT",
        ResourcePermissions.DATA_MANAGE, clusterWriteDisk);
    // This command requires an existing persistent region named "persistentRegion"
    createTestCommand("create region --name=region1234 --template-region=/persistentRegion",
        ResourcePermissions.DATA_MANAGE, clusterWriteDisk);
    createTestCommand("destroy region --name=value", ResourcePermissions.DATA_MANAGE);

    // Data Commands
    createTestCommand("rebalance --include-region=RegionA", ResourcePermissions.DATA_MANAGE);
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
    // createTestCommand("deploy --jar=group1_functions.jar --group=Group1",
    // ResourcePermissions.DATA_MANAGE); // TODO:
    // this command will fail in GfshCommandsSecurityTest at interceptor for jar file checking
    createTestCommand("undeploy --group=Group1", clusterManageDeploy);

    // Diskstore Commands
    createTestCommand("backup disk-store --dir=foo", ResourcePermissions.DATA_READ,
        clusterWriteDisk);
    createTestCommand("list disk-stores", ResourcePermissions.CLUSTER_READ);
    createTestCommand("create disk-store --name=foo --dir=bar", clusterManageDisk);
    createTestCommand("compact disk-store --name=foo", clusterManageDisk);
    createTestCommand("compact offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("upgrade offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("describe disk-store --name=foo --member=baz",
        ResourcePermissions.CLUSTER_READ);
    createTestCommand("revoke missing-disk-store --id=foo", clusterManageDisk);
    createTestCommand("show missing-disk-stores", ResourcePermissions.CLUSTER_READ);
    createTestCommand("describe offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz");
    createTestCommand("validate offline-disk-store --name=foo --disk-dirs=bar");
    createTestCommand("alter disk-store --name=foo --region=xyz --disk-dirs=bar");
    createTestCommand("destroy disk-store --name=foo", clusterManageDisk);

    // CloseDurableClientCommand, CloseDurableCQsCommand, CountDurableCQEventsCommand,
    // ListDurableClientCQsCommand
    createTestCommand("close durable-client --durable-client-id=client1", clusterManageQuery);
    createTestCommand("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1",
        clusterManageQuery);
    createTestCommand("show subscription-queue-size --durable-client-id=client1",
        ResourcePermissions.CLUSTER_READ);
    createTestCommand("list durable-cqs --durable-client-id=client1",
        ResourcePermissions.CLUSTER_READ);

    // ExportImportSharedConfigurationCommands
    createTestCommand("export cluster-configuration --zip-file-name=mySharedConfig.zip",
        ResourcePermissions.CLUSTER_READ);
    createTestCommand("import cluster-configuration --zip-file-name=value.zip",
        ResourcePermissions.CLUSTER_MANAGE);

    createTestCommand("execute function --id=InterestCalculations --groups=Group1");
    createTestCommand("list functions", ResourcePermissions.CLUSTER_READ);

    // GfshHelpCommand, GfshHintCommand
    createTestCommand("hint");
    createTestCommand("help");

    // ClearDefinedIndexesCommand, CreateDefinedIndexesCommand, CreateIndexCommand,
    // DefineIndexCommand, DestroyIndexCommand, ListIndexCommand
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

    // GfshCommand, StartLocatorCommand, StartServerCommand
    createTestCommand("start jconsole");
    createTestCommand("start jvisualvm");
    createTestCommand("start locator --name=locator1");
    createTestCommand("start pulse");
    createTestCommand("start server --name=server1");
    createTestCommand("start vsd");
    createTestCommand("status locator");
    createTestCommand("status server");

    // DescribeMemberCommand, ListMembersCommand
    createTestCommand("describe member --name=server1", ResourcePermissions.CLUSTER_READ);
    createTestCommand("list members", ResourcePermissions.CLUSTER_READ);

    // Misc Commands
    createTestCommand("change loglevel --loglevel=severe --members=server1",
        ResourcePermissions.CLUSTER_WRITE);
    createTestCommand("export logs --dir=data/logs", ResourcePermissions.CLUSTER_READ);
    createTestCommand("export stack-traces --file=stack.txt", ResourcePermissions.CLUSTER_READ);
    createTestCommand("gc", ResourcePermissions.CLUSTER_MANAGE);
    createTestCommand("netstat --member=server1", ResourcePermissions.CLUSTER_READ);
    createTestCommand("show dead-locks --file=deadlocks.txt", ResourcePermissions.CLUSTER_READ);
    createTestCommand("show log --member=locator1 --lines=5", ResourcePermissions.CLUSTER_READ);
    createTestCommand("show metrics", ResourcePermissions.CLUSTER_READ);


    // PDX Commands
    createTestCommand("configure pdx --read-serialized=true", ResourcePermissions.CLUSTER_MANAGE);
    createTestCommand(
        "pdx rename --old=org.apache --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1");

    // Queue Commands
    createTestCommand("create async-event-queue --id=myAEQ --listener=myApp.myListener",
        clusterManageDeploy);
    createTestCommand(
        "create async-event-queue --id=myAEQ --listener=myApp.myListener --persistent",
        clusterManageDeploy, clusterWriteDisk);

    createTestCommand("list async-event-queues", ResourcePermissions.CLUSTER_READ);

    // DescribeRegionCommand, ListRegionCommand
    createTestCommand("describe region --name=value", ResourcePermissions.CLUSTER_READ);
    createTestCommand("list regions", ResourcePermissions.CLUSTER_READ);

    // StatusClusterConfigServiceCommand
    createTestCommand("status cluster-config-service", ResourcePermissions.CLUSTER_READ);

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
    createTestCommand("list gateways", ResourcePermissions.CLUSTER_READ);
    createTestCommand("create gateway-receiver", clusterManageGateway);
    createTestCommand("start gateway-receiver", clusterManageGateway);
    createTestCommand("stop gateway-receiver", clusterManageGateway);
    createTestCommand("status gateway-receiver", ResourcePermissions.CLUSTER_READ);
    createTestCommand("status gateway-sender --id=sender1", ResourcePermissions.CLUSTER_READ);

    // ShellCommand
    createTestCommand("disconnect");

    // JNDI Commands
    createTestCommand(
        "create jndi-binding --name=jndi1 --type=SIMPLE --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\"",
        ResourcePermissions.CLUSTER_MANAGE);
    createTestCommand("list jndi-binding", ResourcePermissions.CLUSTER_READ);
  }
}
