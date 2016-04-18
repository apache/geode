package com.gemstone.gemfire.management.internal.security;

import static org.junit.Assert.*;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class GfshCommandsSecurityTest {
  private static int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
  private static int jmxPort = ports[0];
  private static int httpPort = ports[1];

  private HeadlessGfsh gfsh = null;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxPort, httpPort, "cacheServer.json");

  @Rule
  public GfshShellConnectionRule gfshConnection = new GfshShellConnectionRule(jmxPort, httpPort, true);

  @Before
  public void before(){
    gfsh = gfshConnection.getGfsh();
  }

  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "wrongPwd")
  public void testInvalidCredentials() throws Exception {
    assertFalse(gfshConnection.isAuthenticated());
  }

  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testValidCredentials() throws Exception{
    assertTrue(gfshConnection.isAuthenticated());
  }

  @Test
  @JMXConnectionConfiguration(user = "cluster-reader", password = "1234567")
  public void testAuthorized() throws Exception{
    CommandResult result = null;
//    List<TestCommand> commands = TestCommand.getCommandsOfPermission("DATA:READ");
//    for(TestCommand command:commands){
//      System.out.println("Processing command: "+command.getCommand());
//      gfsh.executeCommand(command.getCommand());
//      result = (CommandResult)gfsh.getResult();
//      System.out.println(result);
//    }
//
//    List<TestCommand> others = TestCommand.getCommands();
//    others.removeAll(commands);
//    for(TestCommand command:others){
//      gfsh.executeCommand(command.getCommand());
//      result = (CommandResult)gfsh.getResult();
//      System.out.println(result);
//    }
    gfsh.executeCommand("describe config --member=Member1");
    result = (CommandResult)gfsh.getResult();
    System.out.println("result is: "+ result);
  }

  @Test
  @JMXConnectionConfiguration(user = "cluster-reader", password = "1234567")
  public void testNotAuthorized() throws Exception{
    CommandResult result = null;
    gfsh.executeCommand("alter runtime --member=server1 --log-level=finest --enable-statistics=true");
    result = (CommandResult)gfsh.getResult();
    System.out.println("result is: "+ result);
  }
}
