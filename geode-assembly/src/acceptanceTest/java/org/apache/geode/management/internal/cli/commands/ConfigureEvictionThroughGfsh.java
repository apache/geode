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

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

// GEODE-1897 Users should be able to configure eviction through gfsh
public class ConfigureEvictionThroughGfsh {

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Test
  public void configureEvictionByEntryCount() throws Exception {

    GfshExecution execution = GfshScript
        .of("start locator --name=locator", "start server --name=server --server-port=0",
            "create region --name=region1 --eviction-action=local-destroy --eviction-entry-count=1000 --type=REPLICATE",
            "create region --name=region2 --eviction-action=overflow-to-disk --eviction-entry-count=1000 --type=REPLICATE",
            "create region --name=region3 --eviction-action=overflow-to-disk --eviction-entry-count=1000 --type=REPLICATE_PERSISTENT",
            "create region --name=region4 --eviction-action=local-destroy --eviction-entry-count=1000 --type=LOCAL",
            "create region --name=region5 --eviction-action=overflow-to-disk --eviction-entry-count=1000 --type=LOCAL")
        .execute(gfsh);

    assertThat(execution.getOutputText()).contains("Region \"/region1\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region2\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region3\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region4\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region5\" created on \"server\"");

    execution = GfshScript
        .of("connect --locator=localhost[10334]",
            "create region --name=region6 --eviction-action=local-destroy --eviction-entry-count=1000 --type=REPLICATE_PERSISTENT")
        .expectFailure().execute(gfsh);
    assertThat(execution.getOutputText()).contains(
        "An Eviction Controller with local destroy eviction action is incompatible with");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region1").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region1")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region2").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region2")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region3").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region3")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region4").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region4")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region5").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region5")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

  }

  @Test
  public void configureEvictionByMaxMemory() throws Exception {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator", "start server --name=server --server-port=0",
            "create region --name=region1 --eviction-action=local-destroy --eviction-max-memory=1000 --type=REPLICATE",
            "create region --name=region2 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --type=REPLICATE",
            "create region --name=region3 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --type=REPLICATE_PERSISTENT",
            "create region --name=region4 --eviction-action=local-destroy --eviction-max-memory=1000 --type=LOCAL",
            "create region --name=region5 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --type=LOCAL")
        .execute(gfsh);
    assertThat(execution.getOutputText()).contains("Region \"/region1\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region2\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region3\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region4\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region5\" created on \"server\"");

    execution = GfshScript
        .of("connect --locator=localhost[10334]",
            "create region --name=region6 --eviction-action=local-destroy --eviction-max-memory=1000 --type=REPLICATE_PERSISTENT")
        .expectFailure().execute(gfsh);
    assertThat(execution.getOutputText()).contains(
        "An Eviction Controller with local destroy eviction action is incompatible with");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region1").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region1")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region2").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region2")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region3").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region3")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region4").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region4")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region5").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region5")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");
  }

  private File createJar() throws IOException {
    File jarToDeploy = new File(gfsh.getTemporaryFolder().getRoot(), "ourJar.jar");

    String classContents =
        "import org.apache.geode.cache.util.ObjectSizer; import org.apache.geode.cache.Declarable;public class MySizer implements ObjectSizer, Declarable { public int sizeof(Object o) { return 10; } }";

    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(jarToDeploy, classContents);

    return jarToDeploy;
  }

  @Test
  public void configureEvictionByObjectSizer() throws Exception {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator", "start server --name=server --server-port=0",
            "sleep --time=1",
            "deploy --jar=" + createJar().getAbsolutePath(),
            "create region --name=region1 --eviction-action=local-destroy --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE",
            "create region --name=region2 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE",
            "create region --name=region3 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE_PERSISTENT",
            "create region --name=region4 --eviction-action=local-destroy --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=LOCAL",
            "create region --name=region5 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=LOCAL")
        .execute(gfsh);

    assertThat(execution.getOutputText()).contains("Region \"/region1\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region2\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region3\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region4\" created on \"server\"");
    assertThat(execution.getOutputText()).contains("Region \"/region5\" created on \"server\"");

    execution = GfshScript
        .of("connect --locator=localhost[10334]",
            "create region --name=region6 --eviction-action=local-destroy --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE_PERSISTENT")
        .expectFailure().execute(gfsh);
    assertThat(execution.getOutputText()).contains(
        "An Eviction Controller with local destroy eviction action is incompatible with");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region1").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region1")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region2").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region2")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region3").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region3")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region4").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region4")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[10334]", "describe region --name=region5").execute(gfsh);
    assertThat(execution.getOutputText()).containsPattern("region5")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

  }
}
