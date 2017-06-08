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

import org.apache.commons.io.IOUtils;
import org.apache.geode.test.dunit.rules.RealGfshRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class SpikingOutGfshTest {
    private static final File GEODE_HOME = new File("/Users/jstewart/projects/open/geode-assembly/build/install/apache-geode");

//  private static final File GEODE_HOME = new File(System.getenv("GEODE_HOME"));
  private static final String GFSH_PATH = GEODE_HOME.toPath().resolve("bin/gfsh").toString();

  @Rule
  public RealGfshRule realGfshRule = new RealGfshRule(GFSH_PATH);

  @Test
  public void testGfsh() throws Exception {
    Process locatorProcess = realGfshRule.executeCommandsAndWaitFor(2, TimeUnit.MINUTES, "start locator --name=locator1");
assertThat(locatorProcess.exitValue()).isEqualTo(0);
//    assertThat(locatorProcess.waitFor(2, TimeUnit.MINUTES)).isTrue();
    Process gfshProcess =  realGfshRule.executeCommands("connect");


    assertThat(gfshProcess.waitFor(10, TimeUnit.SECONDS)).isTrue();
    assertThat(gfshProcess.exitValue()).isEqualTo(0);
  }
}
