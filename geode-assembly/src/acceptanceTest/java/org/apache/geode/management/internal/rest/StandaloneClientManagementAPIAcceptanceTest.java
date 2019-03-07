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

package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.rules.gfsh.internal.ProcessLogger;
import org.apache.geode.util.test.TestUtil;

public class StandaloneClientManagementAPIAcceptanceTest {

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void clientCreatesRegionUsingClusterManagementService() throws Exception {
    JarBuilder jarBuilder = new JarBuilder();
    String filePath =
        TestUtil.getResourcePath(this.getClass(), "/ManagementClientCreateRegion.java");
    assertThat(filePath).as("java file resource not found").isNotBlank();

    File outputJar = new File(tempDir.getRoot(), "output.jar");
    jarBuilder.buildJar(outputJar, new File(filePath));

    GfshExecution startCluster =
        GfshScript.of("start locator", "start server --locators=localhost[10334]")
            .withName("startCluster").execute(gfsh);

    assertThat(startCluster.getProcess().exitValue())
        .as("Cluster did not start correctly").isEqualTo(0);

    Process process = launchClientProcess(outputJar);

    boolean exited = process.waitFor(10, TimeUnit.SECONDS);
    assertThat(exited).as("Process did not exit within 10 seconds").isTrue();
    assertThat(process.exitValue()).as("Process did not exit with 0 return code").isEqualTo(0);

    GfshExecution listRegionsResult = GfshScript.of("connect", "list regions")
        .withName("listRegions").execute(gfsh);
    assertThat(listRegionsResult.getOutputText()).contains("REGION1");
  }

  private Process launchClientProcess(File outputJar) throws IOException {
    Path javaBin = Paths.get(System.getProperty("java.home"), "bin", "java");

    ProcessBuilder pBuilder = new ProcessBuilder();
    pBuilder.directory(tempDir.newFolder());

    StringBuilder classPath = new StringBuilder();
    for (String module : Arrays.asList(
        "commons-logging",
        "commons-lang3",
        "geode-common",
        "geode-management",
        "jackson-annotations",
        "jackson-core",
        "jackson-databind",
        "spring-beans",
        "spring-core",
        "spring-web")) {
      classPath.append(getJarOrClassesForModule(module));
      classPath.append(File.pathSeparator);
    }

    classPath.append(File.pathSeparator);
    classPath.append(outputJar.getAbsolutePath());

    pBuilder.command(javaBin.toString(), "-classpath", classPath.toString(),
        "ManagementClientTestCreateRegion", "REGION1");

    Process process = pBuilder.start();
    new ProcessLogger(process, "clientCreateRegion");
    return process;
  }

  private String getJarOrClassesForModule(String module) {
    String classPath = Arrays.stream(System.getProperty("java.class.path")
        .split(File.pathSeparator))
        .filter(x -> x.contains(module)
            && (x.endsWith("/classes") || x.endsWith("/resources") || x.endsWith(".jar")))
        .collect(Collectors.joining(File.pathSeparator));

    assertThat(classPath).as("no classes found for module: " + module)
        .isNotBlank();

    return classPath;
  }
}
