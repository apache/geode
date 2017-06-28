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

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.rules.gfsh.GfshRule;
import org.apache.geode.test.dunit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class StatusLocatorRealGfshTest {
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void statusLocatorSucceedsWhenConnected() throws Exception {
    GfshScript.of("start locator --name=locator1").execute(gfshRule);

    GfshScript.of("connect", "status locator --name=locator1").execute(gfshRule);
  }

  @Test
  public void statusLocatorFailsWhenNotConnected() throws Exception {
    GfshScript.of("start locator --name=locator1").withName("start locator").execute(gfshRule);

    GfshScript.of("status locator --name=locator1").withName("status locator").expectFailure()
        .execute(gfshRule);
  }
}
