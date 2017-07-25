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

import static org.assertj.core.api.Assertions.assertThat;

import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;

import org.apache.geode.management.internal.cli.util.CommandStringBuilder;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.dunit.rules.gfsh.GfshExecution;
import org.apache.geode.test.dunit.rules.gfsh.GfshRule;
import org.apache.geode.test.dunit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.categories.AcceptanceTest;
import java.util.concurrent.TimeUnit;

@Category(AcceptanceTest.class)
public class StatusLocatorRealGfshTest {
  private int port;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setup() {
    port = getRandomAvailablePort(SOCKET);
  }

  @Test
  public void statusLocatorSucceedsWhenConnected() throws Exception {
    GfshScript.of("start locator --name=locator1 --port=" + Integer.valueOf(port))
        .execute(gfshRule);

    GfshScript.of("connect --locator=192.168.1.66[" + Integer.valueOf(port) + "]",
        "status locator --name=locator1").execute(gfshRule);
  }

  @Test
  public void multilineStatusLocatorByNameWhenNotConnected() throws Exception {
    CommandStringBuilder csb =
        new CommandStringBuilder("status locator").addNewLine().addOption("name", "locator1");

    GfshScript.of("start locator --name=locator1 --port=" + Integer.valueOf(port))
        .awaitAtMost(1, TimeUnit.MINUTES).execute(gfshRule);

    GfshScript.of(csb.toString()).awaitAtMost(1, TimeUnit.MINUTES).expectFailure()
        .execute(gfshRule);

  }

  @Test
  public void statusLocatorByNameFailsWhenNotConnected() throws Exception {
    GfshExecution gfshExecution =
        GfshScript.of("start locator --name=locator1 --port=" + Integer.valueOf(port))
            .withName("start locator").execute(gfshRule);
    GfshScript.of("status locator --name=locator1").withName("status locator").expectFailure()
        .execute(gfshRule);
  }
}
