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
package org.apache.geode.management.internal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.hamcrest.text.IsEqualIgnoringCase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.internal.GemFireVersion;

public class AgentUtilJUnitTest {

  private AgentUtil agentUtil;
  private String version;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws IOException {
    version = GemFireVersion.getGemFireVersion();
    agentUtil = new AgentUtil(version);
  }

  @Test
  public void testRESTApiExists() {
    String gemFireWarLocation = agentUtil.findWarLocation("geode-web-api");
    assertNotNull("GemFire REST API WAR File was not found", gemFireWarLocation);
  }

  @Test
  public void testPulseWarExists() {
    String gemFireWarLocation = agentUtil.findWarLocation("geode-pulse");
    assertNotNull("Pulse WAR File was not found", gemFireWarLocation);
  }

  @Test
  public void testLookupOfWarFileOnClassPath() {
    String classpath = System.getProperty("java.class.path");
    String gemFireWarLocation = agentUtil.findWarLocation("testWarFile");
    assertNull(gemFireWarLocation);

    classpath =
        classpath + System.getProperty("path.separator") + "somelocation/testWarFile.war";
    System.setProperty("java.class.path", classpath);
    gemFireWarLocation = agentUtil.findWarLocation("testWarFile");
    assertNotNull(gemFireWarLocation);
    assertThat(gemFireWarLocation,
        IsEqualIgnoringCase.equalToIgnoringCase("somelocation/testWarFile.war"));
  }
}
