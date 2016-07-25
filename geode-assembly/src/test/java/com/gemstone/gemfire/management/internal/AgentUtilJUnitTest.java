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
package com.gemstone.gemfire.management.internal;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class AgentUtilJUnitTest {

  private AgentUtil agentUtil;
  private String version;

  @Before
  public void setUp() {
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
}
