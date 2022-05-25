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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class StatusLocatorRealGfshTest {

  private int locatorPort;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void setUp() {
    locatorPort = getRandomAvailableTCPPort();
  }

  @Test
  public void statusLocatorSucceedsWhenConnected() {
    GfshScript
        .of("start locator --name=locator1 --port=" + locatorPort)
        .execute(gfshRule);

    GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "status locator --name=locator1")
        .execute(gfshRule);
  }

  @Test
  public void statusLocatorFailsWhenNotConnected() {
    GfshScript
        .of("start locator --name=locator1 --port=" + locatorPort)
        .withName("start-locator")
        .execute(gfshRule);

    GfshScript
        .of("status locator --name=locator1")
        .withName("status-locator")
        .expectFailure()
        .execute(gfshRule);
  }
}
