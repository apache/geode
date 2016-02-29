package com.gemstone.gemfire.management.internal.security;

import java.util.ArrayList;
import java.util.List;

public class TestCommand {
  
  private static List<TestCommand> testCommands = new ArrayList<>();

  static{
    init();
  }
  
  private final String command;
  private final String permission;
  
  public TestCommand(String command, String permission) {
    this.command = command;
    this.permission = permission;
  }
  
  private static void createTestCommand(String command, String permission) {
    TestCommand instance = new TestCommand(command, permission);
    testCommands.add(instance);
  }
  
  public String getCommand() {
    return this.command;
  }

  public String getPermission() {
    return this.permission;
  }

  public static List<TestCommand> getCommands(){
    return testCommands;
  }

  public static List<TestCommand> getCommandsOfPermission(String permission){
    List<TestCommand> result = new ArrayList<>();
    for(TestCommand testCommand:testCommands){
      String cPerm = testCommand.getPermission();
      if(cPerm!=null && cPerm.startsWith(permission)){
        result.add(testCommand);
      }
    }
    return result;
  }

  private static void init() {
    // ClientCommands
    createTestCommand("list clients", "CLUSTER:READ");
    createTestCommand("describe client --clientID=172.16.196.144", "CLUSTER:READ");

    // ConfigCommands
    createTestCommand("alter runtime", "CLUSTER:MANAGE");
    createTestCommand("describe config --member=Member1", "CLUSTER:READ");
    createTestCommand("export config --member=member1", "CLUSTER:READ");

    //CreateAlterDestroyRegionCommands
    createTestCommand("alter region --name=region1 --eviction-max=5000", "DATA:MANAGE");
    createTestCommand("create region --name=region12 --type=REPLICATE", "DATA:MANAGE");
    createTestCommand("destroy region --name=value", "DATA:MANAGE");

    //Data Commands
    createTestCommand("rebalance --include-region=region1", "DATA:MANAGE");
    createTestCommand("export data --region=region1 --file=export.txt --member=exportMember", "DATA:READ");
    createTestCommand("import data --region=region1 --file=import.txt --member=importMember", "DATA:WRITE");
    createTestCommand("put --key=key1 --value=value1 --region=region1", "DATA:WRITE");
    createTestCommand("get --key=key1 --region=region1", "DATA:READ");
    createTestCommand("remove --region=region1", "DATA:MANAGE");
    createTestCommand("query --query='SELECT * FROM /region1'", "DATA:READ");
    createTestCommand("locate entry --key=k1 --region=secureRegion", "DATA:READ");

    // Deploy commands
    //createTestCommand("deploy --jar=group1_functions.jar --group=Group1", "DATA:MANAGE"); // TODO: this command will fail in GfshCommandsSecurityTest at interceptor for jar file checking
    createTestCommand("undeploy --group=Group1", "DATA:MANAGE");

    // Diskstore Commands
    createTestCommand("backup disk-store --dir=foo", "DATA:READ");
    createTestCommand("list disk-stores", "CLUSTER:READ");
    createTestCommand("create disk-store --name=foo --dir=bar", "DATA:MANAGE");
    createTestCommand("compact disk-store --name=foo", "DATA:MANAGE");
    createTestCommand("compact offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("upgrade offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("describe disk-store --name=foo --member=baz", "CLUSTER:READ");
    createTestCommand("revoke missing-disk-store --id=foo", "DATA:MANAGE");
    createTestCommand("show missing-disk-stores", "CLUSTER:READ");
    createTestCommand("describe offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz", null);
    createTestCommand("validate offline-disk-store --name=foo --disk-dirs=bar", null);
    createTestCommand("alter disk-store --name=foo --region=xyz --disk-dirs=bar", null);
    createTestCommand("destroy disk-store --name=foo", "DATA:MANAGE");

    // DurableClientCommands
    createTestCommand("close durable-client --durable-client-id=client1", "DATA:MANAGE");
    createTestCommand("close durable-cq --durable-client-id=client1 --durable-cq-name=cq1", "DATA:MANAGE");
    createTestCommand("show subscription-queue-size --durable-client-id=client1", "CLUSTER:READ");
    createTestCommand("list durable-cqs --durable-client-id=client1", "CLUSTER:READ");

    //ExportIMportSharedConfigurationCommands
    createTestCommand("export cluster-configuration --zip-file-name=mySharedConfig.zip", "CLUSTER:READ");
    createTestCommand("import cluster-configuration --zip-file-name=value.zip", "CLUSTER:MANAGE");

    //FunctionCommands
    //createTestCommand("destroy function --id=InterestCalculations", "DATA:MANAGE");
    createTestCommand("execute function --id=InterestCalculations --group=Group1", "DATA:WRITE");
    createTestCommand("list functions", "CLUSTER:READ");

    //GfshHelpCommands
    createTestCommand("hint", null);
    createTestCommand("help", null);

    //IndexCommands
    createTestCommand("clear defined indexes", "DATA:MANAGE");
    createTestCommand("create defined indexes", "DATA:MANAGE");
    createTestCommand("create index --name=myKeyIndex --expression=region1.Id --region=region1 --type=key", "DATA:MANAGE");
    createTestCommand("define index --name=myIndex1 --expression=exp1 --region=/exampleRegion", "DATA:MANAGE");
    createTestCommand("destroy index --member=server2", "DATA:MANAGE");
    createTestCommand("list indexes", "CLUSTER:READ");

    //LauncherLifecycleCommands
    createTestCommand("start jconsole", null);
    createTestCommand("start jvisualvm", null);
    createTestCommand("start locator --name=locator1", null);
    createTestCommand("start pulse", null);
    createTestCommand("start server --name=server1", null);
    createTestCommand("start vsd", null);
    createTestCommand("status locator", null);
    createTestCommand("status server", null);
    //createTestCommand("stop locator --name=locator1", "CLUSTER:MANAGE");
    //createTestCommand("stop server --name=server1", "CLUSTER:MANAGE");

    //MemberCommands
    createTestCommand("describe member --name=server1", "CLUSTER:READ");
    createTestCommand("list members", "CLUSTER:READ");

    // Misc Commands
    createTestCommand("change loglevel --loglevel=severe --member=server1", "CLUSTER:WRITE");
    createTestCommand("export logs --dir=data/logs", "CLUSTER:READ");
    createTestCommand("export stack-traces --file=stack.txt", "CLUSTER:READ");
    createTestCommand("gc", "CLUSTER:MANAGE");
    createTestCommand("netstat --member=server1", "CLUSTER:READ");
    createTestCommand("show dead-locks --file=deadlocks.txt", "CLUSTER:READ");
    createTestCommand("show log --member=locator1 --lines=5", "CLUSTER:READ");
    createTestCommand("show metrics", "CLUSTER:READ");


    // PDX Commands
    createTestCommand("configure pdx --read-serialized=true", "DATA:MANAGE");
    //createTestCommand("pdx rename --old=com.gemstone --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1", "DATA:MANAGE");

    // Queue Commands
    createTestCommand("create async-event-queue --id=myAEQ --listener=myApp.myListener", "DATA:MANAGE");
    createTestCommand("list async-event-queues", "CLUSTER:READ");

    //RegionCommands
    createTestCommand("describe region --name=value", "CLUSTER:READ");
    createTestCommand("list regions", "CLUSTER:READ");

    // StatusCommands
    createTestCommand("status cluster-config-service", "CLUSTER:READ");

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
    createTestCommand("create gateway-sender --id=sender1 --remote-distributed-system-id=2", "DATA:MANAGE");
    createTestCommand("start gateway-sender --id=sender1", "DATA:MANAGE");
    createTestCommand("pause gateway-sender --id=sender1", "DATA:MANAGE");
    createTestCommand("resume gateway-sender --id=sender1", "DATA:MANAGE");
    createTestCommand("stop gateway-sender --id=sender1", "DATA:MANAGE");
    createTestCommand("load-balance gateway-sender --id=sender1", "DATA:MANAGE");
    createTestCommand("list gateways", "CLUSTER:READ");
    createTestCommand("create gateway-receiver", "DATA:MANAGE");
    createTestCommand("start gateway-receiver", "DATA:MANAGE");
    createTestCommand("stop gateway-receiver", "DATA:MANAGE");
    createTestCommand("status gateway-receiver", "CLUSTER:READ");
    createTestCommand("status gateway-sender --id=sender1", "CLUSTER:READ");

    //ShellCommand
    createTestCommand("disconnect", null);
    //Misc commands
    //createTestCommand("shutdown", "CLUSTER:MANAGE");
  };
}
