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

import static org.junit.Assert.*;
import static com.gemstone.gemfire.security.JSONAuthorization.*;

import java.util.List;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.ErrorResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class GfshCommandsSecurityTest {
  protected static int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
  protected static int jmxPort = ports[0];
  protected static int httpPort = ports[1];

  private HeadlessGfsh gfsh = null;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxPort, httpPort, CACHE_SERVER_JSON);

  @Rule
  public GfshShellConnectionRule gfshConnection;

  public GfshCommandsSecurityTest(){
    gfshConnection = new GfshShellConnectionRule(jmxPort, httpPort, false);
  }


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
  public void testClusterReader() throws Exception{
    runCommandsWithAndWithout("CLUSTER:READ");
  }

  @Test
  @JMXConnectionConfiguration(user = "cluster-writer", password = "1234567")
  public void testClusterWriter() throws Exception{
    runCommandsWithAndWithout("CLUSTER:WRITE");
  }

  @Test
  @JMXConnectionConfiguration(user = "cluster-manager", password = "1234567")
  public void testClusterManager() throws Exception{
    runCommandsWithAndWithout("CLUSTER:MANAGE");
  }

  @Test
  @JMXConnectionConfiguration(user = "data-reader", password = "1234567")
  public void testDataReader() throws Exception{
    runCommandsWithAndWithout("DATA:READ");
  }

  @Test
  @JMXConnectionConfiguration(user = "data-writer", password = "1234567")
  public void testDataWriter() throws Exception{
    runCommandsWithAndWithout("DATA:WRITE");
  }

  @Test
  @JMXConnectionConfiguration(user = "data-manager", password = "1234567")
  public void testDataManager() throws Exception{
    runCommandsWithAndWithout("DATA:MANAGE");
  }

  @Test
  @JMXConnectionConfiguration(user = "regionA-reader", password = "1234567")
  public void testRegionAReader() throws Exception{
    runCommandsWithAndWithout("DATA:READ:RegionA");
  }

  @Test
  @JMXConnectionConfiguration(user = "regionA-writer", password = "1234567")
  public void testRegionAWriter() throws Exception{
    runCommandsWithAndWithout("DATA:WRITE:RegionA");
  }

  @Test
  @JMXConnectionConfiguration(user = "regionA-manager", password = "1234567")
  public void testRegionAManager() throws Exception{
    runCommandsWithAndWithout("DATA:MANAGE:RegionA");
  }


  private void runCommandsWithAndWithout(String permission) throws Exception{
    List<TestCommand> allPermitted = TestCommand.getPermittedCommands(new WildcardPermission(permission, true));
    for(TestCommand permitted:allPermitted) {
      LogService.getLogger().info("Processing authorized command: "+permitted.getCommand());

      gfsh.executeCommand(permitted.getCommand());
      CommandResult result = (CommandResult) gfsh.getResult();
      assertNotNull(result);

      if(result.getResultData() instanceof ErrorResultData) {
        assertNotEquals(ResultBuilder.ERRORCODE_UNAUTHORIZED, ((ErrorResultData) result.getResultData()).getErrorCode());
      }
      else{
        assertEquals(Result.Status.OK, result.getStatus()) ;
      }
    }

    List<TestCommand> others = TestCommand.getCommands();
    others.removeAll(allPermitted);
    for(TestCommand other:others) {
      // skip no permission commands
      if(other.getPermission()==null)
        continue;

      LogService.getLogger().info("Processing unauthorized command: "+other.getCommand());
      gfsh.executeCommand(other.getCommand());

      CommandResult result = (CommandResult) gfsh.getResult();
      int errorCode = ((ErrorResultData) result.getResultData()).getErrorCode();

      // for some commands there are pre execution checks to check for user input error, will skip those commands
      if(errorCode==ResultBuilder.ERRORCODE_USER_ERROR){
        LogService.getLogger().info("Skip user error: "+result.getContent());
        continue;
      }

      assertEquals(ResultBuilder.ERRORCODE_UNAUTHORIZED, ((ErrorResultData) result.getResultData()).getErrorCode());
      String resultMessage = result.getContent().toString();
      String permString = other.getPermission().toString();
      assertTrue(resultMessage+" does not contain "+permString,resultMessage.contains(permString));
    }
  }

  @Test
  @JMXConnectionConfiguration(user = "data-user", password = "1234567")
  public void testGetPostProcess() throws Exception {
    gfsh.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfsh.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfsh.executeCommand("put --region=region1 --key=key3 --value=value3");

    //gfsh.executeCommand("get --region=region1 --key=key1");
    gfsh.executeCommand("query --query=\"select * from /region1\"");
  }



}
