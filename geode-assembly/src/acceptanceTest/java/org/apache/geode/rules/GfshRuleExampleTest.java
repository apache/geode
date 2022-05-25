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
package org.apache.geode.rules;

import static java.lang.String.format;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class GfshRuleExampleTest {

  private int locatorPort;
  private int httpPort;
  private int jmxPort;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void setUp() {
    int[] ports = getRandomAvailableTCPPorts(3);
    locatorPort = ports[0];
    httpPort = ports[1];
    jmxPort = ports[2];
  }

  @Test
  public void test() {
    GfshScript
        .of(format(
            "start locator --name=locator --port=%d --http-service-port=%d --J=-Dgeode.jmx-manager-port=%d",
            locatorPort, httpPort, jmxPort),
            "start server --name=server --disable-default-server")
        .execute(gfshRule);
  }
}
