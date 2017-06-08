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
package org.apache.geode.rest.internal.web;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.dunit.rules.RealGfshRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class SpikingOutGfshTest {
  private static final File GEODE_HOME =
      new File("/Users/jstewart/projects/open/geode-assembly/build/install/apache-geode");

  // private static final File GEODE_HOME = new File(System.getenv("GEODE_HOME"));
  private static final String GFSH_PATH = GEODE_HOME.toPath().resolve("bin/gfsh").toString();

  @Rule
  public RealGfshRule realGfshRule = new RealGfshRule(GFSH_PATH);

  @Test
  public void testGfsh() throws Exception {
    // new GfshScript("connect", "statusLocator").awaitAtMost(2, TimeUnit.MINUTES)
    // .expectExitValue(0);
    //
    // realGfshRule.run( gfshScript);

    Process locatorProcess = realGfshRule.executeCommandsAndWaitAtMost(2, TimeUnit.MINUTES,
        "start locator --name=locator1");
    assertThat(locatorProcess.exitValue()).isEqualTo(0);

    Process gfshProcess = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, "connect",
        "status locator --name=locator1");

    assertThat(gfshProcess.exitValue()).isEqualTo(0);
  }
}
