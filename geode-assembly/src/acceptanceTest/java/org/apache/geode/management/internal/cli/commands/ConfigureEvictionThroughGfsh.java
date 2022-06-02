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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class ConfigureEvictionThroughGfsh {

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
  public void configureEvictionByEntryCount() {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator --port=" + locatorPort,
            "start server --name=server --disable-default-server",
            "create region --name=region1 --eviction-action=local-destroy --eviction-entry-count=1000 --type=REPLICATE",
            "create region --name=region2 --eviction-action=overflow-to-disk --eviction-entry-count=1000 --type=REPLICATE",
            "create region --name=region3 --eviction-action=overflow-to-disk --eviction-entry-count=1000 --type=REPLICATE_PERSISTENT",
            "create region --name=region4 --eviction-action=local-destroy --eviction-entry-count=1000 --type=LOCAL",
            "create region --name=region5 --eviction-action=overflow-to-disk --eviction-entry-count=1000 --type=LOCAL")
        .execute(gfshRule);

    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region1\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region2\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region3\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region4\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region5\" created on \"server\"");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "create region --name=region6 --eviction-action=local-destroy --eviction-entry-count=1000 --type=REPLICATE_PERSISTENT")
        .expectFailure()
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .contains("An Eviction Controller with local destroy eviction action is incompatible with");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region1")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region1")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region2")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region2")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region3")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region3")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region4")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region4")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-maximum-value\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region5")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region5")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-maximum-value\\s+ | 1000");
  }

  @Test
  public void configureEvictionByMaxMemory() {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator --port=" + locatorPort,
            "start server --name=server --disable-default-server",
            "create region --name=region1 --eviction-action=local-destroy --eviction-max-memory=1000 --type=REPLICATE",
            "create region --name=region2 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --type=REPLICATE",
            "create region --name=region3 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --type=REPLICATE_PERSISTENT",
            "create region --name=region4 --eviction-action=local-destroy --eviction-max-memory=1000 --type=LOCAL",
            "create region --name=region5 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --type=LOCAL")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region1\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region2\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region3\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region4\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region5\" created on \"server\"");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "create region --name=region6 --eviction-action=local-destroy --eviction-max-memory=1000 --type=REPLICATE_PERSISTENT")
        .expectFailure()
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .contains("An Eviction Controller with local destroy eviction action is incompatible with");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region1")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region1")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region2")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region2")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region3")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region3")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region4")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region4")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region5")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region5")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");
  }

  @Test
  public void configureEvictionByObjectSizer()
      throws IOException {
    GfshExecution execution = GfshScript
        .of("start locator --name=locator --port=" + locatorPort,
            "start server --name=server --disable-default-server",
            "sleep --time=1",
            "deploy --jar=" + createJar().getAbsolutePath(),
            "create region --name=region1 --eviction-action=local-destroy --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE",
            "create region --name=region2 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE",
            "create region --name=region3 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE_PERSISTENT",
            "create region --name=region4 --eviction-action=local-destroy --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=LOCAL",
            "create region --name=region5 --eviction-action=overflow-to-disk --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=LOCAL")
        .execute(gfshRule);

    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region1\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region2\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region3\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region4\" created on \"server\"");
    assertThat(execution.getOutputText())
        .contains("Region \"" + SEPARATOR + "region5\" created on \"server\"");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "create region --name=region6 --eviction-action=local-destroy --eviction-max-memory=1000 --eviction-object-sizer=MySizer --type=REPLICATE_PERSISTENT")
        .expectFailure()
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .contains("An Eviction Controller with local destroy eviction action is incompatible with");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region1")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region1")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region2")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region2")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region3")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region3")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region4")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region4")
        .containsPattern("eviction-action\\s+| local-destroy")
        .containsPattern("eviction-max-memory\\s+ | 1000");

    execution = GfshScript
        .of("connect --locator=localhost[" + locatorPort + "]",
            "describe region --name=region5")
        .execute(gfshRule);
    assertThat(execution.getOutputText())
        .containsPattern("region5")
        .containsPattern("eviction-action\\s+| overflow-to-disk")
        .containsPattern("eviction-max-memory\\s+ | 1000");
  }

  private File createJar() throws IOException {
    File jarToDeploy = folderRule.getFolder().toPath().resolve("ourJar.jar").toFile();

    String classContents =
        "import org.apache.geode.cache.util.ObjectSizer; import org.apache.geode.cache.Declarable;public class MySizer implements ObjectSizer, Declarable { public int sizeof(Object o) { return 10; } }";

    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(jarToDeploy, classContents);

    return jarToDeploy;
  }
}
