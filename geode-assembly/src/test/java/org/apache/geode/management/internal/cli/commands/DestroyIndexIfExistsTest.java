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
import org.junit.experimental.categories.Category;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.categories.AcceptanceTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

@Category(AcceptanceTest.class)
public class DestroyIndexIfExistsTest {

  @Rule
  public GfshRule gfsh = new GfshRule();


  @Test
  public void destroyIndexIfExists() throws Exception {
    GfshExecution execution =
        GfshScript.of("start locator --name=locator", "start server --name=server",
            "sleep --time=1", "destroy index --name=i1 --if-exists=true").execute(gfsh);

    assertThat(execution.getOutputText()).contains("Index i1 not found - skipped");
  }
}
