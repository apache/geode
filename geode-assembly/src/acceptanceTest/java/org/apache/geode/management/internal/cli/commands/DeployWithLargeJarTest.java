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

import static java.util.Objects.requireNonNull;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.lang.SystemUtils.isWindows;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class DeployWithLargeJarTest {

  private int locatorPort;
  // Jakarta EE migration: Added explicit http-service-port to avoid port conflicts
  // with embedded Jetty server used by Jakarta Servlet containers
  private int httpServicePort;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void setUp() {
    locatorPort = getRandomAvailableTCPPort();
    // Jakarta EE migration: Allocate separate port for HTTP service to prevent conflicts
    httpServicePort = getRandomAvailableTCPPort();
  }

  @Test
  public void deployLargeSetOfJars() {
    File libDir = findGfsh().getParent().getParent().resolve("lib").toFile();

    String commonLibs = Arrays
        .stream(requireNonNull(libDir.listFiles(x -> x.getName().startsWith("commons"))))
        .map(File::getAbsolutePath)
        .collect(Collectors.joining(","));

    // Jakarta EE migration: Increased heap from 128m to 256m to accommodate larger Jakarta
    // libraries
    // Added explicit http-service-port configuration to avoid random port conflicts
    GfshScript
        .of("start locator --name=locator --max-heap=256m --port=" + locatorPort
            + " --J=-Dgemfire.http-service-port=" + httpServicePort,
            "start server --name=server --max-heap=256m --disable-default-server",
            "sleep --time=1",
            "deploy --jars=" + commonLibs)
        .execute(gfshRule);
  }

  private static Path findGfsh() {
    Path geodeHome = new RequiresGeodeHome().getGeodeHome().toPath();

    if (isWindows()) {
      return geodeHome.resolve("bin").resolve("gfsh.bat");
    }
    return geodeHome.resolve("bin").resolve("gfsh");
  }
}
