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

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class DeployWithLargeJarTest {

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Test
  public void deployLargeSetOfJars() throws Exception {
    File libDir = gfsh.getGfshPath().getParent().getParent().resolve("lib").toFile();
    String commonLibs = Arrays.stream(libDir.listFiles(x -> x.getName().startsWith("commons")))
        .map(File::getAbsolutePath).collect(Collectors.joining(","));
    GfshExecution execution = GfshScript.of("start locator --name=locator --max-heap=128m",
        "start server --name=server --max-heap=128m --server-port=0", "sleep --time=1",
        "deploy --jars=" + commonLibs).execute(gfsh);
  }

}
