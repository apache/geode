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

import static com.gemstone.gemfire.internal.Assert.*;

import org.apache.geode.security.templates.SamplePostProcessor;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ IntegrationTest.class, SecurityTest.class })
public class GfshCommandsPostProcessorTest {

  protected static int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

  private HeadlessGfsh gfsh = null;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxPort, "com/gemstone/gemfire/management/internal/security/cacheServer.json", SamplePostProcessor.class);

  @Rule
  public GfshShellConnectionRule gfshConnection = new GfshShellConnectionRule(jmxPort);

  @Before
  public void before(){
    gfsh = gfshConnection.getGfsh();
  }

  @Test
  @JMXConnectionConfiguration(user = "data-user", password = "1234567")
  public void testGetPostProcess() throws Exception {
    gfsh.executeCommand("put --region=region1 --key=key1 --value=value1");
    gfsh.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfsh.executeCommand("put --region=region1 --key=key3 --value=value3");

    // for get command, assert the return value is processed
    gfsh.executeCommand("get --region=region1 --key=key1");
    assertTrue(gfsh.outputString.contains("data-user/region1/key1/value1"), gfsh.outputString);

    // for query command, assert the return values are processed
    gfsh.executeCommand("query --query=\"select * from /region1\"");
    assertTrue(gfsh.outputString.contains("data-user/null/null/value1"), gfsh.outputString);
    assertTrue(gfsh.outputString.contains("data-user/null/null/value2"), gfsh.outputString);
    assertTrue(gfsh.outputString.contains("data-user/null/null/value3"), gfsh.outputString);
  }
}
