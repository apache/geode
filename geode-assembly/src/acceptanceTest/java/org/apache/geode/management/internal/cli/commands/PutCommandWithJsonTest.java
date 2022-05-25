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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class PutCommandWithJsonTest {

  private Path jarToDeploy;
  private int locatorPort;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void setup() throws IOException {
    Path rootFolder = folderRule.getFolder().toPath();
    jarToDeploy = rootFolder.resolve("ourJar.jar");

    String classContents =
        "public class Customer implements java.io.Serializable {private String name; public void setName(String name){this.name=name;}}";
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(jarToDeploy.toFile(), classContents);

    locatorPort = getRandomAvailableTCPPort();
  }

  @Test
  public void putWithJsonString() {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator --port=" + locatorPort,
            "start server --name=server --disable-default-server --locators=localhost["
                + locatorPort + "]",
            "sleep --time=1",
            "deploy --jar=" + jarToDeploy,
            "create region --name=region --type=REPLICATE", "sleep --time=1",
            "put --region=region --key=key --value=('name':'Jinmei') --value-class=Customer")
        .execute(gfshRule);

    assertThat(execution.getOutputText())
        .contains("Value Class : Customer")
        .doesNotContain("Couldn't convert JSON to Object");
  }
}
