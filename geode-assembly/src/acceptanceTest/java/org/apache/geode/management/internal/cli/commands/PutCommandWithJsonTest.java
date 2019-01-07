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

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class PutCommandWithJsonTest {

  private File jarToDeploy;

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Before
  public void setup() throws IOException {
    jarToDeploy = new File(gfsh.getTemporaryFolder().getRoot(), "ourJar.jar");

    String classContents =
        "public class Customer implements java.io.Serializable {private String name; public void setName(String name){this.name=name;}}";
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(jarToDeploy, classContents);
  }

  @Test
  public void putWithJsonString() throws Exception {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator", "start server --name=server --server-port=0",
            "sleep --time=1",
            "deploy --jar=" + jarToDeploy.getAbsolutePath(),
            "create region --name=region --type=REPLICATE", "sleep --time=1",
            "put --region=region --key=key --value=('name':'Jinmei') --value-class=Customer")
        .execute(gfsh);

    assertThat(execution.getOutputText()).doesNotContain("Couldn't convert JSON to Object");
    assertThat(execution.getOutputText()).contains("Value Class : Customer");
  }
}
